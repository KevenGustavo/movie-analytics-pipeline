CREATE SCHEMA IF NOT EXISTS `movie-analy.movies_raw_dataset`;

-- Tabela de Filmes (External)
CREATE OR REPLACE EXTERNAL TABLE `movie-analy.movies_raw_dataset.ext_movies`
OPTIONS (
    format = 'CSV',
    uris = ['gs://movielens-bucket-test/raw/movies.csv'],
    skip_leading_rows = 1,
    null_marker = 'NA'
);

-- Tabela Histórico de Avaliações (External)
CREATE OR REPLACE EXTERNAL TABLE `movie-analy.movies_raw_dataset.ext_user_rating_history`
OPTIONS (
    format = 'CSV',
    uris = ['gs://movielens-bucket-test/raw/user_rating_history.csv'],
    skip_leading_rows = 1,
    null_marker = 'NA'
);

-- Tabela Dados de Crença/Expectativa (External)
CREATE OR REPLACE EXTERNAL TABLE `movie-analy.movies_raw_dataset.ext_belief_data`
OPTIONS (
    format = 'CSV',
    uris = ['gs://movielens-bucket-test/raw/belief_data.csv'],
    skip_leading_rows = 1,
    null_marker = 'NA'
);