CREATE TABLE review_facts (
    review_id INT PRIMARY KEY,
    asin_key INT,
    reviewer_key INT,
    time_key INT,
    overall_rating INT,
    helpful_votes INT,
    total_votes INT,
    FOREIGN KEY (asin_key) REFERENCES dim_product(asin_key),
    FOREIGN KEY (reviewer_key) REFERENCES dim_reviewer(reviewer_key),
    FOREIGN KEY (time_key) REFERENCES dim_time(time_key)
);