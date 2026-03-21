-- Connect to the local docker-compose DuckLake instance
LOAD httpfs;
LOAD ducklake;
LOAD postgres;
CREATE SECRET (
    TYPE s3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    ENDPOINT 'localhost:9000',
    USE_SSL false,
    URL_STYLE 'path'
);
ATTACH 'ducklake:postgres:host=localhost port=5433 dbname=ducklake user=ducklake password=ducklake' AS lake (DATA_PATH 's3://ducklake/data');
USE lake;

SELECT count(*) AS total_rows FROM events;
FROM events LIMIT 10;
