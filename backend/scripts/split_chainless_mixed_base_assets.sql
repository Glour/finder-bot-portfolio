-- Split chainless GlobalTokens containing DIFFERENT base_assets
-- Example: GT "ARIX" contains FROX, PEMDAS, TRUST - should be 3 separate GTs
-- Date: 2026-02-19

BEGIN;

-- Step 1: Find all problematic GTs (chainless with multiple base_assets)
WITH problem_gts AS (
    SELECT 
        gt.id as gt_id,
        gt.symbol,
        COUNT(DISTINCT t.base_asset) as base_asset_count
    FROM global_tokens gt
    JOIN token_chains tc ON tc.global_token_id = gt.id
    JOIN tokens t ON t.id = tc.token_id
    WHERE gt.is_deleted = false
        AND tc.contract_address = ''
    GROUP BY gt.id, gt.symbol
    HAVING COUNT(DISTINCT t.base_asset) > 1
),

-- Step 2: Get all base_assets for each problematic GT
base_assets_per_gt AS (
    SELECT 
        pgt.gt_id,
        pgt.symbol,
        t.base_asset,
        ROW_NUMBER() OVER (PARTITION BY pgt.gt_id ORDER BY t.base_asset) as base_asset_rank
    FROM problem_gts pgt
    JOIN token_chains tc ON tc.global_token_id = pgt.gt_id
    JOIN tokens t ON t.id = tc.token_id
    WHERE tc.contract_address = ''
    GROUP BY pgt.gt_id, pgt.symbol, t.base_asset
),

-- Step 3: Create new GTs for each additional base_asset (keep rank=1 in original GT)
new_gts AS (
    INSERT INTO global_tokens (id, symbol, is_deleted, created_at, updated_at)
    SELECT 
        gen_random_uuid(),
        ba.symbol || '_CHAINLESS_' || ba.base_asset,
        false,
        NOW(),
        NOW()
    FROM base_assets_per_gt ba
    WHERE ba.base_asset_rank > 1
    RETURNING id, symbol, substring(symbol from '.*_CHAINLESS_(.*)$') as base_asset
),

-- Step 4: Move token_chains to new GTs
moved_chains AS (
    UPDATE token_chains
    SET global_token_id = subq.new_gt_id
    FROM (
        SELECT 
            tc.id as tc_id,
            ng.id as new_gt_id,
            ng.symbol as new_gt_symbol
        FROM new_gts ng
        JOIN base_assets_per_gt ba ON ba.base_asset = ng.base_asset 
            AND ba.base_asset_rank > 1
        JOIN token_chains tc ON tc.global_token_id = ba.gt_id
            AND tc.contract_address = ''
        JOIN tokens t ON t.id = tc.token_id 
            AND t.base_asset = ng.base_asset
    ) subq
    WHERE token_chains.id = subq.tc_id
    RETURNING token_chains.id, subq.new_gt_id, subq.new_gt_symbol
),

-- Step 5: Copy token_prices to new GTs
prices_copied AS (
    INSERT INTO token_prices (global_token_id, global_token_source_id, token_id, market_type, price_usd, orderbook, created_at, updated_at, get_at)
    SELECT DISTINCT
        mc.new_gt_id,
        tp.global_token_source_id,
        tp.token_id,
        tp.market_type,
        tp.price_usd,
        tp.orderbook,
        tp.created_at,
        tp.updated_at,
        tp.get_at
    FROM moved_chains mc
    JOIN token_chains tc ON tc.id = mc.id
    JOIN token_prices tp ON tp.token_id = tc.token_id
    ON CONFLICT (global_token_id, global_token_source_id, market_type) DO UPDATE
    SET price_usd = EXCLUDED.price_usd,
        updated_at = EXCLUDED.updated_at,
        orderbook = EXCLUDED.orderbook,
        get_at = EXCLUDED.get_at
    RETURNING global_token_id
),

-- Step 6: Add blacklist entries to prevent re-merging
blacklist_inserts AS (
    INSERT INTO global_token_pair_blacklist (id, global_token_a_id, global_token_b_id, reason, created_by, created_at, updated_at)
    SELECT 
        gen_random_uuid(),
        ba1.gt_id,
        ng.id,
        'Chainless tokens with different base_assets - auto-split',
        'split_chainless_mixed_base_assets.sql',
        NOW(),
        NOW()
    FROM new_gts ng
    JOIN base_assets_per_gt ba1 ON ba1.symbol = substring(ng.symbol from '^(.*)_CHAINLESS_')
        AND ba1.base_asset_rank = 1
    ON CONFLICT DO NOTHING
    RETURNING id
)

-- Summary
SELECT 
    (SELECT COUNT(*) FROM problem_gts) as total_problem_gts,
    (SELECT COUNT(*) FROM base_assets_per_gt WHERE base_asset_rank > 1) as new_gts_needed,
    (SELECT COUNT(*) FROM new_gts) as new_gts_created,
    (SELECT COUNT(*) FROM moved_chains) as chains_moved,
    (SELECT COUNT(*) FROM prices_copied) as prices_copied,
    (SELECT COUNT(*) FROM blacklist_inserts) as blacklist_entries;

COMMIT;
