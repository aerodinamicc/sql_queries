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

-------------
-- NEWS
-------------

  CREATE DATABASE news_db

--drop table if exists real_estate_db.daily_measurements
-- aws s3 rm s3://news-scrapping/raw/ --recursive --profile aero

CREATE EXTERNAL TABLE news_db.raw_measurements (
 `comments` string,
 `views` string,
 `shares` string,
 `created_timestamp` string,
 `visited_timestamp` string,
 `tags` string,
 `section` string,
 `title` string,
 `subtitle` string,
 `category` string,
 `link` string,
 `article_text` string,
 `author` string,
 `thumbs_down` string,
 `thumbs_up` string,
 `location` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
   (
   'separatorChar' = '\t',
   'quoteChar' = '"',
   'escapeChar' = '\\')  -- field.delim specifies the delimiter of the data ('/t' for tab delimited)
location 's3://news-scrapping/raw/' -- Specify location of the data in S3 at the bucket-level
TBLPROPERTIES (
  "skip.header.line.count"="1" -- skip.header tells Athena that the first row is a header and to not upload that into the table
  )
  
select * from news_db.raw_measurements
where link like '%sportal.bg%'
limit 20
  
  
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

-- aws s3 rm s3://real-estate-scrapping/processed/ --recursive --profile aero
  
CREATE TABLE IF NOT EXISTS real_estate_db.daily
WITH (partitioned_by = ARRAY['country', 'measurement_day', 'site'], format='parquet', external_location='s3://real-estate-scrapping/processed/') as
with clean as (
SELECT
	link,
	id,
	replace(
		replace(
			replace(
				replace(
					replace(
						lower(trim(type)),
					'1-', 'едно'),
				'2-', 'дву'),
			'3-', 'три'),
		'4-', 'четири'),
	' апартамент', '') as type,
	city,
	trim(
	replace(
		replace(
			replace(
				replace(
					replace(
						replace(lower(trim(place)), 'гр. софия', ''),
					'софийска област', ''),
				'българия', ''), 
			'/', ''),
		',', ' '),
	'близо до', '')
	) as place, --гр. София / ДрагалевциСофийска област, България
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
select 
	link, id, type,
	regexp_like(type, '(?:стаен|мезонет|ателие)') as is_apartment,
	regexp_like(type, '(?:стаен|^къща|^парцел|мезонет|ателие|вила)') as is_type,
	city, place, is_for_sale, price, area, details, labels, year, available_from, views, lon, lat, country, measurement_day, site
from clean


select site, measurement_day, count(*)
from real_estate_db.daily
group by 1, 2
order by 1, 2

select place, count(*) as rows, array_distinct(array_agg(site)) as sites
from real_estate_db.daily 
where country = 'bg'
group by 1
order by 2 desc

select type, is_type, is_apartment, count(*)
from real_estate_db.daily 
where country = 'bg'
and is_type
group by 1, 2, 3
order by 1, 2, 3




