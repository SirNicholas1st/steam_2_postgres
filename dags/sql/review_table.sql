-- create the table to store the reviews

CREATE TABLE IF NOT EXISTS Review_table (
    review_id SERIAL PRIMARY KEY,
    poster VARCHAR NOT NULL,
    found_helpful int NOT NULL,
    hours_on_record FLOAT(2),
    date DATE NOT NULL,
    review_text VARCHAR
)