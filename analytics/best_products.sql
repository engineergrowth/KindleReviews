SELECT
    dp.asin,
    COUNT(rf.review_id) AS total_reviews,
    AVG(rf.overall_rating) AS avg_rating
FROM
    review_facts rf
JOIN
    dim_product dp ON rf.asin_key = dp.asin_key
GROUP BY
    dp.asin
HAVING
    COUNT(rf.review_id) >= 5
ORDER BY
    avg_rating DESC