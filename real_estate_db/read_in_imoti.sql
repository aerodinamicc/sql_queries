--------
-- CREATE MASTER TABLE
--------
drop table if exists imotibg

CREATE TABLE imotibg
(
	LINK varchar, 
	TITLE varchar,
	type varchar,
	ADDRESS varchar, 
	DETAILS json, 
	PLACE varchar, 
	LON float, 
	LAT float, 
	ID varchar, 
	PRICE bigint, 
	PRICE_SQM float, 
	AREA bigint, 
	DESCRIPTION varchar, 
	measurement_day varchar
);

---------------------
---- Append new batch
---------------------

drop table if exists imotibg_import

CREATE TABLE imotibg_import
(
	ADDRESS varchar, 
	PLACE varchar, 
	DETAILS varchar,
	ID bigint, 
	LINK varchar, 
	TITLE varchar,
	LON float, 
	LAT float, 
	DESCRIPTION varchar
);

copy imotibg_import (address, place, details, id, link, title, lon, lat, description) 
FROM 'D:/git/data_collection/real_estate/output/imotibg_2020-07-07.tsv' DELIMITER E'\t' CSV HEADER

select substring(replace(replace(details, '"0"', '0'), '''', '"')::json ->> 'Цена' from '^[\d]+') as float
from imotibg_import 

--------
-- Clean
--------

drop table if exists imotibg_import_casted

CREATE TABLE imotibg_import_casted AS
SELECT 
	link, 
	title, 
	replace(replace(details, '"0"', '0'), '''', '"')::json ->> 'Тип на имота:' as type,
	address, 
	replace(replace(details, '"0"', '0'), '''', '"')::json as details,
	trim(place) as place, 
	lon::float, 
	lat::float,
	id, 
	cast(substring(replace(replace(details, '"0"', '0'), '''', '"')::json ->> 'Цена' from '^[\d]+') as bigint) as price,
	cast(replace(replace(details, '"0"', '0'), '''', '"')::json ->> 'Цена/m2:' as float) as price_sqm,
	cast(substring(replace(replace(details, '"0"', '0'), '''', '"')::json ->> 'Площ:' from '^[\d]+') as bigint) as area,
	trim(substr(description, 2))  as description,
	'2020-07-07' as measurement_day
FROM imotibg_import
	
insert into imotibg (link, title, type, address, details, place, lon, lat, id, price, price_sqm, area, description, measurement_day)
select * from imotibg_import_casted

select measurement_day, count(*) from imotibg group by 1 order by 1

select count(*) from imotibg

select * from imotibg limit 100
