-- Разделяет ВСЕ GT содержащие SPOT и FUTURES на ОДНОЙ бирже
-- Работает напрямую с БД, БЕЗ оглядки на signal_history

DO $$
DECLARE
    r RECORD;
    new_gt_id UUID;
    new_gt_symbol VARCHAR(255);
    futures_tc_ids UUID[];
BEGIN
    -- Находим все GT где SPOT и FUTURES на одной бирже
    FOR r IN (
        WITH gt_exchange_types AS (
            SELECT 
                gt.id as gt_id,
                gt.symbol as gt_symbol,
                t.base_asset,
                s.name as exchange,
                COUNT(DISTINCT CASE WHEN t.market_type = 'SPOT' THEN t.id END) as spot_count,
                COUNT(DISTINCT CASE WHEN t.market_type = 'FUTURES' THEN t.id END) as futures_count
            FROM global_tokens gt
            JOIN token_chains tc ON tc.global_token_id = gt.id
            JOIN tokens t ON t.id = tc.token_id
            JOIN sources s ON s.id = t.source_id
            WHERE gt.is_deleted = false
            GROUP BY gt.id, gt.symbol, t.base_asset, s.name
            HAVING COUNT(DISTINCT t.market_type) > 1  -- И SPOT, и FUTURES
        )
        SELECT * FROM gt_exchange_types
        WHERE spot_count > 0 AND futures_count > 0
        ORDER BY gt_symbol, exchange
    ) LOOP
        RAISE NOTICE '========================================';
        RAISE NOTICE 'GT: % | base: % @ % | SPOT: %, FUTURES: %',
                     r.gt_symbol, r.base_asset, r.exchange, r.spot_count, r.futures_count;
        
        -- Генерируем новый GT symbol
        new_gt_symbol := r.base_asset || '_F_' || UPPER(r.exchange);
        
        -- Проверяем существование
        SELECT id INTO new_gt_id FROM global_tokens 
        WHERE symbol = new_gt_symbol AND is_deleted = false;
        
        -- Если не существует - создаём
        IF new_gt_id IS NULL THEN
            new_gt_id := gen_random_uuid();
            INSERT INTO global_tokens (id, symbol, is_deleted, created_at, updated_at)
            VALUES (new_gt_id, new_gt_symbol, false, now(), now());
            
            RAISE NOTICE '  Created new GT: % (id: %)', new_gt_symbol, new_gt_id;
        ELSE
            RAISE NOTICE '  Using existing GT: % (id: %)', new_gt_symbol, new_gt_id;
        END IF;
        
        -- Собираем ID всех FUTURES token_chains для этого base_asset + exchange
        SELECT ARRAY_AGG(tc.id) INTO futures_tc_ids
        FROM token_chains tc
        JOIN tokens t ON t.id = tc.token_id
        JOIN sources s ON s.id = t.source_id
        WHERE tc.global_token_id = r.gt_id
          AND t.base_asset = r.base_asset
          AND s.name = r.exchange
          AND t.market_type = 'FUTURES';
        
        IF futures_tc_ids IS NULL OR array_length(futures_tc_ids, 1) = 0 THEN
            RAISE NOTICE '  ⚠ No FUTURES token_chains found, skipping';
            CONTINUE;
        END IF;
        
        -- Перемещаем FUTURES token_chains
        UPDATE token_chains
        SET global_token_id = new_gt_id, updated_at = now()
        WHERE id = ANY(futures_tc_ids);
        
        GET DIAGNOSTICS r.futures_count = ROW_COUNT;
        RAISE NOTICE '  ✓ Moved % FUTURES token_chains', r.futures_count;
        
        -- Копируем sources для нового GT
        INSERT INTO global_token_sources (id, global_token_id, source_id, created_at, updated_at)
        SELECT gen_random_uuid(), new_gt_id, source_id, now(), now()
        FROM global_token_sources
        WHERE global_token_id = r.gt_id
        ON CONFLICT DO NOTHING;
        
        -- Перемещаем цены FUTURES
        WITH moved_tokens AS (
            SELECT t.id as token_id
            FROM token_chains tc
            JOIN tokens t ON t.id = tc.token_id
            WHERE tc.id = ANY(futures_tc_ids)
        ),
        latest_prices AS (
            SELECT DISTINCT ON (tp.global_token_source_id, tp.market_type)
                tp.id
            FROM token_prices tp
            WHERE tp.token_id IN (SELECT token_id FROM moved_tokens)
              AND tp.market_type = 'FUTURES'
            ORDER BY tp.global_token_source_id, tp.market_type, tp.updated_at DESC
        )
        UPDATE token_prices
        SET global_token_id = new_gt_id, updated_at = now()
        WHERE id IN (SELECT id FROM latest_prices);
        
        -- Добавляем в blacklist
        INSERT INTO global_token_pair_blacklist (id, global_token_a_id, global_token_b_id, reason, created_at, updated_at)
        VALUES (
            gen_random_uuid(),
            r.gt_id,
            new_gt_id,
            'Auto-split: SPOT+FUTURES on same exchange (' || r.exchange || ')',
            now(),
            now()
        )
        ON CONFLICT DO NOTHING;
        
        RAISE NOTICE '  ✓ Split complete: % (SPOT) + % (FUTURES)', r.gt_symbol, new_gt_symbol;
    END LOOP;
    
    RAISE NOTICE '========================================';
    RAISE NOTICE '✓ Done! Restart spread collectors and flush Redis.';
END $$;
