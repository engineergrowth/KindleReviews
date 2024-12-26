{{ config(
    materialized='incremental',
    unique_key='asin'
) }}

SELECT
    asin,
    COUNT(*) AS total_reviews,
    CAST(ROUND(AVG(overall), 3) AS FLOAT64) AS avg_rating
FROM
    `{{ target.project }}.{{ target.dataset }}.kindle_reviews`
GROUP BY
    asin
HAVING
    total_reviews >= 5
ORDER BY
    avg_rating DESC;
