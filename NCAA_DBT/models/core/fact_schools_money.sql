{{ config(materialized='table') }}

with schools_lookup as (
    select * 
    from {{ ref('stg_schools_lookup') }}
    where cast(UNITID as STRING) != ''
), 

schools_money as (
    select *
    from {{ ref('stg_schools_money') }}
    where cast(ipeds_id as STRING) != ''
)


select lk.unitid, lk.instnm, lk.stabbr, lk.city, lk.zip, money.year, money.revenue_in_mil
from schools_lookup lk
inner join schools_money money
on lk.unitid = money.ipeds_id