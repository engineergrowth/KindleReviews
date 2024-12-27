{{ config(
    materialized='incremental',
    unique_key='asin'
) }}

SELECT
    asin,
    COUNT(*) AS review_count_last_week,
    CAST(ROUND(AVG(overall), 3) AS FLOAT64) AS avg_rating_last_week
FROM
    `{{ target.project }}.{{ target.dataset }}.kindle_reviews`
WHERE
    PARSE_DATE('%m %d, %Y', reviewTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY
    asin
ORDER BY
    review_count_last_week DESC
