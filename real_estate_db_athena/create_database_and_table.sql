CREATE DATABASE real_estate_db

--drop table if exists real_estate_db.daily_measurements

CREATE EXTERNAL TABLE real_estate_db.daily_measurements (
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
`lat` string
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