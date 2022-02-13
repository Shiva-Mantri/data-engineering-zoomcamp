-- Question 1: What is count for fhv vehicles data for year 2019 *
SELECT count(*) FROM `nyc-taxi-rides-338810.ntr_bigquery_data_all.external_fhv_tripdata` LIMIT 1000;

-- Question 2: How many distinct dispatching_base_num we have in fhv for 2019 *
SELECT count(distinct(dispatching_base_num)) FROM `nyc-taxi-rides-338810.ntr_bigquery_data_all.external_fhv_tripdata` LIMIT 1000;

-- Question 3: Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num *
-- There is varied # of dispatching_base_num. Hence best strategy is Partition by dropoff_datetime and cluster by dispatching_base_num

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE nyc-taxi-rides-338810.ntr_bigquery_data_all.fhv_tripdata_partitoned_clustered
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM nyc-taxi-rides-338810.ntr_bigquery_data_all.external_fhv_tripdata;

-- Question 4: What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279 *
SELECT count(*) as trips
FROM nyc-taxi-rides-338810.ntr_bigquery_data_all.fhv_tripdata_partitoned_clustered
WHERE DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
AND dispatching_base_num in ('B00987', 'B02060', 'B02279');

-- Question 5: What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag *
SELECT count(distinct(SR_Flag)) FROM `nyc-taxi-rides-338810.ntr_bigquery_data_all.external_fhv_tripdata` LIMIT 1000;

