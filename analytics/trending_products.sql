SELECT
    dp.asin,
    COUNT(rf.review_id) AS review_count_last_week,
    AVG(rf.overall_rating) AS avg_rating_last_week
FROM
    review_facts rf
JOIN
    dim_product dp ON rf.asin_key = dp.asin_key
JOIN
    dim_time dt ON rf.time_key = dt.time_key
WHERE
    DATE(FROM_UNIXTIME(dt.unixReviewTime)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY
    dp.asin
ORDER BY
    review_count_last_week DESC