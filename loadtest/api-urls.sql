-- URLs for api-bench.sh: both endpoints for the most recently terminated
-- (cached) experiments, like the HTTP load generator queries.
WITH recent AS (
    SELECT experiment_id FROM experiments_terminated ORDER BY terminated_at DESC LIMIT 2000
), ranges AS (
    SELECT experiment_id, min(timestamp) AS lo, max(timestamp) AS hi, random() AS a, random() AS b
    FROM recent JOIN measurements USING (experiment_id) GROUP BY experiment_id
)
SELECT 'http://localhost:3003/temperature/out-of-range?experiment-id=' || experiment_id FROM ranges
UNION ALL
SELECT format('http://localhost:3003/temperature?experiment-id=%s&start-time=%s&end-time=%s',
              experiment_id, floor((lo + (hi - lo) * least(a, b)) * 1000) / 1000,
              ceil((lo + (hi - lo) * greatest(a, b)) * 1000) / 1000)
FROM ranges;
