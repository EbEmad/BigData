CREATE EXTERNAL TABLE IF NOT EXISTS dim_user_staging(
    login STRING,
    premium_user BOOLEAN,
    address STRING,
    phone STRING,
    name STRING,
    surname STRING,
    year_of_birth INT
)
ROW FORMAT 