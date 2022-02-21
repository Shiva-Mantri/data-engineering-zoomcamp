-------------------------------------------------
-- All data between '2019-01-01' and '2020-12-31' and VendorID is not null

----------------------
-- RAW DATA COUNTS  --
----------------------
-- Taxi Zone Lookup - Total Records - 265
SELECT count(*) FROM `nyc-taxi-rides-338810.ntr_bigquery_data_all.taxi_zone_lookup`;

-- Yellow - Total Count - 109,001,563, Vendor ID is not null - 107,948,798
SELECT count(*) FROM `nyc-taxi-rides-338810.ntr_bigquery_data_all.yellow_tripdata`
where tpep_pickup_datetime between '2019-01-01' and '2020-12-31' and VendorID is not null;

-- Green - Total Count - 7,775,180, Vendor ID is not null - 6,834,120
SELECT count(*) FROM `nyc-taxi-rides-338810.ntr_bigquery_data_all.green_tripdata`
where lpep_pickup_datetime between '2019-01-01' and '2020-12-31' and VendorID is not null;

-- fhv_tripdata - Total Count - 42,084,899 and dispatch base num is not null 42,084,899
SELECT count(*) FROM `nyc-taxi-rides-338810.ntr_bigquery_data_all.fhv_tripdata`
where pickup_datetime between '2019-01-01' and '2020-12-31' and 
(dispatching_base_num is not null or dispatching_base_num <> '');

----------------------
-- STG DATA COUNTS  --
----------------------
-- Yellow - Total Count - 56,065,351
SELECT count(*) FROM `nyc-taxi-rides-338810.ntr_bigquery_data_all.stg_yellow_tripdata`
where pickup_datetime between '2019-01-01' and '2020-12-31';

-- Green - Total Count - 6,303,038
SELECT count(*) FROM `nyc-taxi-rides-338810.ntr_bigquery_data_all.stg_green_tripdata`
where  pickup_datetime between '2019-01-01' and '2020-12-31';

-- fhv - Total Count - 36,091,586
SELECT count (*) FROM `nyc-taxi-rides-338810.ntr_bigquery_data_all.stg_fhv_tripdata`
where pickup_datetime between '2019-01-01' and '2020-12-31';


----------------------
-- FACT COUNTS  --
----------------------
-- Fact - Total Count - 79,090,039
SELECT count(*) FROM `nyc-taxi-rides-338810.ntr_bigquery_data_all.fact_trips`
where pickup_datetime between '2019-01-01' and '2020-12-31';

-- Fact - Total Count - 17,536,081
SELECT count(*) FROM `nyc-taxi-rides-338810.ntr_bigquery_data_all.fact_fhv_trips`
where pickup_datetime between '2019-01-01' and '2020-12-31';
