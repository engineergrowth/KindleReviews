{{ config(
    materialized='incremental',
    unique_key='asin'
) }}

WITH recent_reviews AS (
    SELECT
        asin,
        COUNT(*) AS total_reviews,
        COUNTIF(overall <= 2) AS negative_reviews
    FROM
        `{{ target.project }}.{{ target.dataset }}.kindle_reviews`
    WHERE
        review_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY
        asin
),

flagged_products AS (
    SELECT
        asin,
        total_reviews,
        negative_reviews,
        SAFE_DIVIDE(negative_reviews, total_reviews) * 100 AS negative_review_percentage
    FROM
        recent_reviews
)

SELECT
    asin,
    total_reviews,
    negative_reviews,
    negative_review_percentage
FROM
    flagged_products
WHERE
    total_reviews >= 10
    AND negative_review_percentage >= 30  -- Flag products with >=30% negative reviews
ORDER BY
    negative_review_percentage DESC;
