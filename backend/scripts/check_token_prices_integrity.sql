-- Check token_prices integrity (2026-02-19)
-- Finds records where global_token_id in token_prices doesn't match token_chains
-- Run periodically to detect if the bug returns

-- 1. Count mismatches
SELECT 
    'Total mismatches' as check_type,
    COUNT(*) as count
FROM token_prices tp
JOIN token_chains tc ON tc.token_id = tp.token_id
WHERE tc.global_token_id != tp.global_token_id

UNION ALL

-- 2. Show examples by source
SELECT 
    CONCAT('Mismatches in ', s.name) as check_type,
    COUNT(*) as count
FROM token_prices tp
JOIN tokens t ON t.id = tp.token_id
JOIN sources s ON s.id = t.source_id
JOIN token_chains tc ON tc.token_id = tp.token_id
WHERE tc.global_token_id != tp.global_token_id
GROUP BY s.name
ORDER BY count DESC;

-- 3. Show top 10 problematic tokens
SELECT 
    t.symbol,
    t.base_asset,
    s.name as source,
    (SELECT symbol FROM global_tokens WHERE id = tc.global_token_id) as correct_gt,
    (SELECT symbol FROM global_tokens WHERE id = tp.global_token_id) as wrong_gt,
    tp.updated_at
FROM token_prices tp
JOIN tokens t ON t.id = tp.token_id
JOIN sources s ON s.id = t.source_id
JOIN token_chains tc ON tc.token_id = tp.token_id
WHERE tc.global_token_id != tp.global_token_id
ORDER BY tp.updated_at DESC
LIMIT 10;
