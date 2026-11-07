-- Автоматическое разделение проблемных SPOT+FUTURES merge
-- Список проблемных пар (symbol, base_asset, exchange):
-- MSTR_F0/MSTR@xt, BMT_F10/BMT@xt, STO/STO@xt, CORX/AIA@xt, 
-- FLR/ETHFI@xt, GRASS/GRASS@ascendex, и др.

-- Для каждой пары:
-- 1. Создаём новый GT для FUTURES
-- 2. Перемещаем FUTURES token_chain на новый GT
-- 3. Копируем sources
-- 4. Перемещаем prices
-- 5. Добавляем в blacklist

DO $$
DECLARE
    r RECORD;
    new_gt_id UUID;
    new_gt_symbol VARCHAR(255);
    spot_gt_id UUID;
    futures_gt_id UUID;
    spot_token_id UUID;
    futures_token_id UUID;
    spot_tc_id UUID;
    futures_tc_id UUID;
BEGIN
    -- Находим все проблемные пары
    FOR r IN (
        WITH bad_signals AS (
            SELECT DISTINCT
                sh.symbol,
                CASE 
                    WHEN sh.buy_from ~* '^spot_' THEN SUBSTRING(sh.buy_from FROM '^spot_(.*)$')
                    WHEN sh.buy_from ~* '^futures_' THEN SUBSTRING(sh.buy_from FROM '^futures_(.*)$')
                END as exchange,
                MAX(sh.spread_percent) as max_spread
            FROM signal_history sh
            WHERE sh.created_at > now() - interval '24 hours'
              AND sh.spread_percent > 2000
              AND (
                  (sh.buy_from ~* '^spot_' AND sh.sell_to ~* '^futures_' 
                   AND SUBSTRING(sh.buy_from FROM '^spot_(.*)$') = SUBSTRING(sh.sell_to FROM '^futures_(.*)$'))
                  OR
                  (sh.buy_from ~* '^futures_' AND sh.sell_to ~* '^spot_'
                   AND SUBSTRING(sh.buy_from FROM '^futures_(.*)$') = SUBSTRING(sh.sell_to FROM '^spot_(.*)$'))
              )
            GROUP BY sh.symbol, exchange
            HAVING MAX(sh.spread_percent) > 2000
        )
        SELECT DISTINCT
            bs.symbol,
            bs.exchange,
            bs.max_spread,
            gt.id as gt_id,
            gt.symbol as gt_symbol,
            t.base_asset,
            t.id as token_id,
            tc.id as tc_id,
            t.market_type
        FROM bad_signals bs
        JOIN tokens t ON UPPER(t.base_asset) = UPPER(bs.symbol)
        JOIN sources s ON s.id = t.source_id AND s.name = bs.exchange
        JOIN token_chains tc ON tc.token_id = t.id
        JOIN global_tokens gt ON gt.id = tc.global_token_id
        WHERE gt.is_deleted = false
        ORDER BY gt.symbol, t.market_type
    ) LOOP
        -- Получаем SPOT token
        SELECT t2.id, tc2.id INTO spot_token_id, spot_tc_id
        FROM tokens t2
        JOIN sources s2 ON s2.id = t2.source_id
        JOIN token_chains tc2 ON tc2.token_id = t2.id
        WHERE tc2.global_token_id = r.gt_id
          AND t2.base_asset = r.base_asset
          AND s2.name = r.exchange
          AND t2.market_type = 'SPOT'
        LIMIT 1;
        
        -- Получаем FUTURES token
        SELECT t2.id, tc2.id INTO futures_token_id, futures_tc_id
        FROM tokens t2
        JOIN sources s2 ON s2.id = t2.source_id
        JOIN token_chains tc2 ON tc2.token_id = t2.id
        WHERE tc2.global_token_id = r.gt_id
          AND t2.base_asset = r.base_asset
          AND s2.name = r.exchange
          AND t2.market_type = 'FUTURES'
        LIMIT 1;
        
        -- Пропускаем если нет SPOT или FUTURES
        IF spot_token_id IS NULL OR futures_token_id IS NULL THEN
            CONTINUE;
        END IF;
        
        -- Генерируем symbol для нового GT
        new_gt_symbol := r.base_asset || '_F_SPLIT_' || UPPER(r.exchange);
        
        -- Проверяем существование GT с таким symbol
        SELECT id INTO new_gt_id FROM global_tokens 
        WHERE symbol = new_gt_symbol AND is_deleted = false;
        
        -- Если не существует - создаём
        IF new_gt_id IS NULL THEN
            new_gt_id := gen_random_uuid();
            INSERT INTO global_tokens (id, symbol, is_deleted, created_at, updated_at)
            VALUES (new_gt_id, new_gt_symbol, false, now(), now());
            
            RAISE NOTICE 'Created GT: % (%) for %@%', new_gt_symbol, new_gt_id, r.base_asset, r.exchange;
        ELSE
            RAISE NOTICE 'Using existing GT: % (%) for %@%', new_gt_symbol, new_gt_id, r.base_asset, r.exchange;
        END IF;
        
        -- Перемещаем FUTURES token_chain
        UPDATE token_chains
        SET global_token_id = new_gt_id, updated_at = now()
        WHERE id = futures_tc_id;
        
        -- Копируем sources для нового GT
        INSERT INTO global_token_sources (id, global_token_id, source_id, created_at, updated_at)
        SELECT gen_random_uuid(), new_gt_id, source_id, now(), now()
        FROM global_token_sources
        WHERE global_token_id = r.gt_id
        ON CONFLICT DO NOTHING;
        
        -- Перемещаем цены FUTURES (просто меняем global_token_id для последней цены)
        UPDATE token_prices tp
        SET global_token_id = new_gt_id, updated_at = now()
        WHERE tp.id = (
            SELECT id FROM token_prices
            WHERE token_id = futures_token_id
            ORDER BY updated_at DESC
            LIMIT 1
        );
        
        -- Добавляем в blacklist
        INSERT INTO global_token_pair_blacklist (id, global_token_a_id, global_token_b_id, reason, created_at, updated_at)
        VALUES (
            gen_random_uuid(),
            r.gt_id,
            new_gt_id,
            'Auto-split: SPOT vs FUTURES anomaly (>2000%) on ' || r.exchange,
            now(),
            now()
        )
        ON CONFLICT DO NOTHING;
        
        RAISE NOTICE 'Split: % (SPOT) + % (FUTURES) for %@%', 
                     r.gt_symbol, new_gt_symbol, r.base_asset, r.exchange;
    END LOOP;
    
    RAISE NOTICE 'Done! Restart spread collectors and flush Redis.';
END $$;
