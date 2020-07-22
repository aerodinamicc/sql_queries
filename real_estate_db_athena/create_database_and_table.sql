CREATE DATABASE real_estate_db

--drop table if exists real_estate_db.daily_measurements

CREATE EXTERNAL TABLE real_estate_db.raw_measurements (
`link` string,
`id` string,
`type` string,
`city` string,
`place` string,
`is_for_sale` string,
`price` string,
`area` string,
`details` string,
`labels` string,
`year` string,
`available_from` string,
`views` string,
`lon` string,
`lat` string,
`measurement_day` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
   (
   'separatorChar' = '\t',
   'quoteChar' = '"',
   'escapeChar' = '\\')  -- field.delim specifies the delimiter of the data ('/t' for tab delimited)
location 's3://real-estate-scrapping/raw/' -- Specify location of the data in S3 at the bucket-level
TBLPROPERTIES (
  "skip.header.line.count"="1" -- skip.header tells Athena that the first row is a header and to not upload that into the table
  )
  
  
---------------
-- Etuovi details
---------------
  
CREATE EXTERNAL TABLE real_estate_db.etuovi_details (
`link` string,
`selling_price` string,
`debt_component` string,
`total_price` string,
`monthly_fee` string,
`maintainance_fee` string,
`financial_fee` string,
`floor` string,
`communications` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
   (
   'separatorChar' = '\t',
   'quoteChar' = '"',
   'escapeChar' = '\\')  -- field.delim specifies the delimiter of the data ('/t' for tab delimited)
location 's3://real-estate-scrapping/etuovi_details/' -- Specify location of the data in S3 at the bucket-level
TBLPROPERTIES (
  "skip.header.line.count"="1" -- skip.header tells Athena that the first row is a header and to not upload that into the table
  )
  
---------------------
---------Additional partition
drop table if exists real_estate_db.daily
  
CREATE TABLE IF NOT EXISTS real_estate_db.daily
WITH (partitioned_by = ARRAY['country', 'measurement_day', 'site'], format='parquet', external_location='s3://real-estate-scrapping/processed/') as
with clean as (
SELECT
	link,
	id,
	lower(trim(type)) as type,
	city,
	lower(trim(place)) as place,
	try_cast(is_for_sale as boolean) as is_for_sale,
	try_cast(replace(price, ' ') as double) as price,
	try_cast(replace(area, ' ') as double) as area,
	details,
	labels,
	try_cast(year as double) as year,
	available_from,
	try_cast(views as double) as views,
	try_cast(replace(lon, ',', '.') as double) as lon,
	try_cast(replace(lat, ',', '.') as double) as lat,
	case when url_extract_host(link) in ('etuovi.com', 'www.vuokraovi.com') then 'fi' else 'bg' end as country,
	date(measurement_day) as measurement_day,
	regexp_extract(url_extract_host(link), '(?:www\.)?(.*)', 1) as site
from real_estate_db.raw_measurements
where try_cast(replace(price, ' ') as double) is not null
)
select *
from clean


select site, measurement_day, count(*)
from real_estate_db.daily
group by 1, 2

select url_extract_host(link), measurement_day, count(*)
from real_estate_db.daily_measurements
group by 1, 2



