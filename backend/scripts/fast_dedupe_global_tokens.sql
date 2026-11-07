-- Fast deduplication: keep only ONE GT per symbol (the best one)
-- This is much faster than merge (seconds vs hours)

-- Step 1: Mark duplicates as deleted (keep the "best" GT for each symbol)
WITH ranked_gts AS (
    SELECT 
        id,
        symbol,
        ROW_NUMBER() OVER (
            PARTITION BY symbol 
            ORDER BY 
                (SELECT COUNT(*) FROM global_token_sources WHERE global_token_id = global_tokens.id) DESC,  -- Most sources
                (SELECT COUNT(*) FROM token_chains WHERE global_token_id = global_tokens.id) DESC,          -- Most chains
                created_at ASC                                                                                -- Oldest
        ) as rank
    FROM global_tokens
    WHERE is_deleted = false
)
UPDATE global_tokens
SET is_deleted = true,
    updated_at = now()
WHERE id IN (
    SELECT id FROM ranked_gts WHERE rank > 1
);

-- Step 2: Show stats
SELECT 
    'Total GT' as metric,
    COUNT(*) as count
FROM global_tokens
UNION ALL
SELECT 
    'Active GT' as metric,
    COUNT(*) as count
FROM global_tokens
WHERE is_deleted = false
UNION ALL
SELECT 
    'Deleted (dedupe)' as metric,
    COUNT(*) as count
FROM global_tokens
WHERE is_deleted = true;
