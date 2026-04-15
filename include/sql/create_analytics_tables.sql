CREATE SCHEMA IF NOT EXISTS `movie-analy.movies_analytics_dataset`;

-- Tabela Dimensão: dim_movies 
CREATE OR REPLACE TABLE `movie-analy.movies_analytics_dataset.dim_movies` AS
SELECT
    CAST(movieId AS INT64) AS movie_id,
    
    TRIM(REGEXP_REPLACE(title, r'\s\(\d{4}\)$', '')) AS clean_title,
    
    CAST(REGEXP_EXTRACT(title, r'\((\d{4})\)') AS INT64) AS release_year,
    
    genres
FROM `movie-analy.movies_raw_dataset.ext_movies`
WHERE movieId IS NOT NULL;

-- Tabela Fato: fact_ratings
CREATE OR REPLACE TABLE `movie-analy.movies_analytics_dataset.fact_ratings` AS
SELECT
    CAST(userId AS INT64) AS user_id,
    CAST(movieId AS INT64) AS movie_id,
    CAST(rating AS FLOAT64) AS rating,
    DATETIME(tstamp) AS rating_datetime
FROM `movie-analy.movies_raw_dataset.ext_user_rating_history`
WHERE userId IS NOT NULL AND movieId IS NOT NULL;