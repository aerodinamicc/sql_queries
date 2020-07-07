drop table if exists  yavlena

CREATE TABLE yavlena
(
	LINK varchar, 
	TYPE varchar,
	IS_FOR_SALE bool,
	EXTRAS json, 
	PLACE varchar, 
	LON float, 
	LAT float, 
	ID varchar, 
	PRICE float, 
	PRICE_SQM float, 
	AREA bigint, 
	DESCRIPTION varchar,
	measurement_day varchar
);

----------------------
---- Append new batch
----------------------

drop table if exists yavlena_import

CREATE TABLE yavlena_import
(
	LINK varchar, 
	TYPE varchar,
	EXTRAS varchar, 
	PLACE varchar, 
	LON varchar, 
	LAT varchar, 
	PRICE varchar, 
	AREA varchar, 
	DESCRIPTION varchar
);

copy yavlena_import (link, type, extras, place, lon, lat, price, area, description) 
FROM 'D:/git/data_collection/real_estate/yavlena_0707.tsv' DELIMITER E'\t' CSV HEADER

select * from yavlena_import limit 100

--------
-- Clean
--------

drop table if exists yavlena_import_casted

CREATE TABLE yavlena_import_casted AS
SELECT 
	link, type, 
	not lower(link) ~ 'rent' as is_for_sale,
	replace(extras, '''', '"')::json as extras,
	trim(place) as place, 
	replace(lon, ',', '.')::float as lon, 
	replace(lat, ',', '.')::float as lat,
	substring(link from '[\d]+') as id,
	substring(price, 2)::bigint as price,
	round(substring(price, 2)::bigint / regexp_replace(area, '\D', '', 'g')::bigint, 2) as price_sqm,
	regexp_replace(area, '\D', '', 'g')::bigint as area,
	description,
	'2020-07-06' as measurement_day
FROM yavlena_import

select * from yavlena_import_casted limit 100
						 

insert into yavlena (link, type, is_for_sale, extras, place, lon, lat, id, price, price_sqm, area, description, measurement_day)
select * from yavlena_import_casted

select * from yavlena limit 100