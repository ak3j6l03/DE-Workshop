-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp.external_yellow_tripdata_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-zoomcamp-zoe-demo/yellow_tripdata_2024-*.parquet']
);

-- Check yellow trip data
SELECT * FROM zoomcamp.external_yellow_tripdata_2024 limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE zoomcamp.yellow_tripdata_2024_non_partitioned AS
SELECT * FROM zoomcamp.external_yellow_tripdata_2024;

-- Count of records for the 2024 Yellow Taxi Data
SELECT COUNT(*) AS total_records
FROM `zoomcamp.external_yellow_tripdata_2024`;

-- Distinct PULocationIDs in external table
SELECT COUNT(DISTINCT PULocationID) AS distinct_pu_count
FROM `zoomcamp.external_yellow_tripdata_2024`;

-- Distinct PULocationIDs in non-partitioned table
SELECT COUNT(DISTINCT PULocationID) AS distinct_pu_count
FROM `zoomcamp.yellow_tripdata_2024_non_partitioned`;

-- Retrieve PULocationID
SELECT PULocationID
FROM `zoomcamp.yellow_tripdata_2024_non_partitioned`;

-- Retrieve PULocationID and DOLocationID
SELECT PULocationID, DOLocationID
FROM `zoomcamp.yellow_tripdata_2024_non_partitioned`;

-- Counting zero fare trips
SELECT COUNT(*) AS zero_fare_count
FROM `zoomcamp.external_yellow_tripdata_2024`
WHERE fare_amount = 0;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE zoomcamp.yellow_tripdata_2024_partitioned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM zoomcamp.external_yellow_tripdata_2024;

-- Select from non-partitioned table
SELECT DISTINCT(VendorID)
FROM zoomcamp.yellow_tripdata_2024_non_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Select from partitioned table
SELECT DISTINCT(VendorID)
FROM zoomcamp.yellow_tripdata_2024_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15';