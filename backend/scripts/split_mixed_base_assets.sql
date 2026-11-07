-- Разделяет GT содержащие РАЗНЫЕ base_asset
-- Создаёт отдельный GT для каждого уникального base_asset

DO $$
DECLARE
    r RECORD;
    base_rec RECORD;
    new_gt_id UUID;
    new_symbol VARCHAR(255);
    primary_base VARCHAR(255);
    moved_token_count INT;
BEGIN
    -- Находим все GT с multiple base_asset
    FOR r IN (
        WITH gt_bases AS (
            SELECT 
                gt.id as gt_id,
                gt.symbol as gt_symbol,
                t.base_asset,
                COUNT(t.id) as token_count
            FROM global_tokens gt
            JOIN token_chains tc ON tc.global_token_id = gt.id
            JOIN tokens t ON t.id = tc.token_id
            WHERE gt.is_deleted = false
            GROUP BY gt.id, gt.symbol, t.base_asset
        ),
        gt_summary AS (
            SELECT 
                gt_id,
                gt_symbol,
                COUNT(DISTINCT base_asset) as base_count,
                ARRAY_AGG(base_asset ORDER BY token_count DESC) as base_assets,
                SUM(token_count) as total_tokens
            FROM gt_bases
            GROUP BY gt_id, gt_symbol
            HAVING COUNT(DISTINCT base_asset) > 1
        )
        SELECT * FROM gt_summary
        ORDER BY base_count DESC, gt_symbol
    ) LOOP
        RAISE NOTICE '========================================';
        RAISE NOTICE 'Processing GT: % (id: %)', r.gt_symbol, r.gt_id;
        RAISE NOTICE 'Contains % base_assets: %', r.base_count, r.base_assets;
        
        -- Берём первый (самый частый) base_asset как primary
        primary_base := r.base_assets[1];
        
        RAISE NOTICE 'Primary base_asset: %', primary_base;
        
        -- Для каждого НЕ-primary base_asset создаём новый GT
        FOR base_rec IN (
            SELECT DISTINCT t.base_asset
            FROM token_chains tc
            JOIN tokens t ON t.id = tc.token_id
            WHERE tc.global_token_id = r.gt_id
              AND t.base_asset != primary_base
        ) LOOP
            -- Считаем сколько токенов у этого base_asset
            SELECT COUNT(DISTINCT t.id) INTO moved_token_count
            FROM token_chains tc
            JOIN tokens t ON t.id = tc.token_id
            WHERE tc.global_token_id = r.gt_id
              AND t.base_asset = base_rec.base_asset;
            
            -- Генерируем новый symbol
            new_symbol := base_rec.base_asset || '_SPLIT';
            
            -- Проверяем существование
            SELECT id INTO new_gt_id 
            FROM global_tokens 
            WHERE symbol = new_symbol AND is_deleted = false;
            
            -- Если не существует - создаём
            IF new_gt_id IS NULL THEN
                new_gt_id := gen_random_uuid();
                INSERT INTO global_tokens (id, symbol, is_deleted, created_at, updated_at)
                VALUES (new_gt_id, new_symbol, false, now(), now());
                
                RAISE NOTICE '  Created new GT: % (%) for base_asset: % (%  tokens)', 
                             new_symbol, new_gt_id, base_rec.base_asset, moved_token_count;
            ELSE
                RAISE NOTICE '  Using existing GT: % (%) for base_asset: %', 
                             new_symbol, new_gt_id, base_rec.base_asset;
            END IF;
            
            -- Перемещаем token_chains для этого base_asset
            UPDATE token_chains tc
            SET global_token_id = new_gt_id, updated_at = now()
            FROM tokens t
            WHERE tc.token_id = t.id
              AND tc.global_token_id = r.gt_id
              AND t.base_asset = base_rec.base_asset;
            
            -- Копируем sources для нового GT
            INSERT INTO global_token_sources (id, global_token_id, source_id, created_at, updated_at)
            SELECT gen_random_uuid(), new_gt_id, source_id, now(), now()
            FROM global_token_sources
            WHERE global_token_id = r.gt_id
            ON CONFLICT DO NOTHING;
            
            -- Перемещаем только ПОСЛЕДНЮЮ цену для каждого (source, market_type)
            WITH tokens_to_move AS (
                SELECT t.id as token_id
                FROM tokens t
                JOIN token_chains tc ON tc.token_id = t.id
                WHERE tc.global_token_id = new_gt_id
                  AND t.base_asset = base_rec.base_asset
            ),
            latest_prices AS (
                SELECT DISTINCT ON (global_token_source_id, market_type)
                    id, global_token_id, global_token_source_id, token_id,
                    market_type, price_usd, orderbook, get_at, created_at, updated_at
                FROM token_prices
                WHERE token_id IN (SELECT token_id FROM tokens_to_move)
                ORDER BY global_token_source_id, market_type, updated_at DESC
            )
            UPDATE token_prices tp
            SET global_token_id = new_gt_id, updated_at = now()
            WHERE tp.id IN (SELECT id FROM latest_prices);
            
            -- Добавляем в blacklist
            INSERT INTO global_token_pair_blacklist (id, global_token_a_id, global_token_b_id, reason, created_at, updated_at)
            VALUES (
                gen_random_uuid(),
                r.gt_id,
                new_gt_id,
                'Auto-split: Mixed base_asset (' || primary_base || ' vs ' || base_rec.base_asset || ')',
                now(),
                now()
            )
            ON CONFLICT DO NOTHING;
            
            RAISE NOTICE '  ✓ Moved % tokens from % to %', moved_token_count, r.gt_symbol, new_symbol;
        END LOOP;
        
        RAISE NOTICE '✓ Finished processing %', r.gt_symbol;
    END LOOP;
    
    RAISE NOTICE '========================================';
    RAISE NOTICE '✓ Done! Restart spread collectors and flush Redis.';
END $$;
