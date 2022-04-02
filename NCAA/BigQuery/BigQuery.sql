-- 234 - distinct IPEDS_ID
-- CREATE EXTERNAL TABLE - NCAA Money
CREATE OR REPLACE EXTERNAL TABLE `ncaa-data-eng-zc-2022.ncaa_datawarehouse_bigquery.ext_money`
OPTIONS (
  format = 'CSV',
  uris = ['gs://ncaa-datalake-bucket_ncaa-data-eng-zc-2022/raw/College_Sports_Money.csv']
);

-- 6440
-- CREATE EXTERNAL TABLE - State
CREATE OR REPLACE EXTERNAL TABLE `ncaa-data-eng-zc-2022.ncaa_datawarehouse_bigquery.ext_schools`
OPTIONS (
  format = 'CSV',
  uris = ['gs://ncaa-datalake-bucket_ncaa-data-eng-zc-2022/raw/School_Data.csv']
);

-- CREATE PARTITION BY YEAR and CLUSTER BY CONF
CREATE OR REPLACE TABLE ncaa-data-eng-zc-2022.ncaa_datawarehouse_bigquery.ncaa_money
PARTITION BY YEAR
CLUSTER BY revenue_mil_num
AS
SELECT IPEDS_ID, data as team,PARSE_DATE("%Y", CAST(YEAR AS STRING)) as YEAR
, (Other_Revenue + Corporate_Sponsorship__Advertising__Licensing + Donor_Contributions +
Competition_Guarantees + NCAA_Conference_Distributions__Media_Rights__and_Post_Season_Football+ 
Ticket_Sales + Institutional_Government_Support + Student_Fees)/1000000 as revenue_in_mil,
CAST(((Other_Revenue + Corporate_Sponsorship__Advertising__Licensing + Donor_Contributions +
Competition_Guarantees + NCAA_Conference_Distributions__Media_Rights__and_Post_Season_Football+ 
Ticket_Sales + Institutional_Government_Support + Student_Fees)/1000000) as NUMERIC) as revenue_mil_num
 FROM ncaa-data-eng-zc-2022.ncaa_datawarehouse_bigquery.ext_money;

-- CREATE LOOKUP TABLE
CREATE OR REPLACE TABLE ncaa-data-eng-zc-2022.ncaa_datawarehouse_bigquery.schools_lookup
as
select * from ncaa-data-eng-zc-2022.ncaa_datawarehouse_bigquery.ext_schools;