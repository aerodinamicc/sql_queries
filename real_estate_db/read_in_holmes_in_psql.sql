--------
-- CREATE MASTER TABLE
--------
drop table if exists holmes

CREATE TABLE holmes
(
	LINK varchar, 
	TITLE varchar,
	ADDRESS varchar, 
	DETAILS json, 
	REGION varchar,
	PLACE varchar, 
	LON float, 
	LAT float, 
	ID varchar, 
	PRICE float, 
	PRICE_SQM float, 
	CURRENCY varchar,
	AREA bigint, 
	FLOOR bigint,
	DESCRIPTION varchar, 
	VIEWS bigint, 
	DATE varchar, 
	AGENCY varchar,
	measurement_day varchar
);

----------------------
---- Append new batch
----------------------

drop table if exists holmes_import

CREATE TABLE holmes_import
(
	LINK varchar, 
	TITLE varchar,
	ADDRESS varchar, 
	DETAILS varchar, 
	PLACE varchar, 
	LON varchar, 
	LAT varchar, 
	ID varchar, 
	PRICE varchar, 
	PRICE_SQM varchar, 
	AREA varchar, 
	FLOOR varchar,
	DESCRIPTION varchar, 
	VIEWS varchar, 
	DATE varchar, 
	AGENCY varchar, 
	POLY varchar
);

/*
F:\202006_verto\imoti\holmes.bg_0321.tsv
F:\202006_verto\imoti\holmes.bg_0405.tsv
F:\202006_verto\imoti\holmes.bg_0425.tsv
F:\202006_verto\imoti\holmes.bg_0509.tsv
F:\202006_verto\imoti\holmes.bg_0529.tsv
F:\202006_verto\imoti\holmes.bg_0605.tsv
D:/git/data_collection/real_estate/output/holmes.bg_0625.tsv
D:/git/data_collection/real_estate/output/holmes.bg_0706.tsv
D:/git/data_collection/real_estate/output/holmes_2020-07-18.tsv
*/

copy holmes_import (link, title, address, details, place, lon, lat, id, price, price_sqm, area, floor, description, views, date, agency, poly) 
FROM 'D:/git/data_collection/real_estate/output/holmes.bg_0625.tsv' DELIMITER E'\t' CSV HEADER

select * from holmes_import limit 100


CREATE FUNCTION ROUND(float,int) RETURNS NUMERIC AS $$
   SELECT ROUND($1::numeric,$2);
$$ language SQL IMMUTABLE;


drop table if exists holmes_import_casted

CREATE TABLE holmes_import_casted AS
SELECT 
	link, title, 
	substring(address from trim(place)||'(.*)') as address, 
	details::json,
	substring(address from '(.*)'||trim(place)) as region, 
	trim(place), lon::float, lat::float,
	id, 
	case when lower(price) like '%лв%' then round(replace(substring(price from '[\d\s]+'), ' ', '')::float / 1.9588, 2)
		else replace(substring(trim(price) from '[\d\s]+'), ' ', '')::float 
		END as price, 
	case when price_sqm like '%лв%' then round(substring(price_sqm from '[\d\.]+')::float / 1.9588, 2)
		else substring(price_sqm from '[\d\.]+')::float 
		END as price_sqm,
	/*case when price like '%лв%' then 'BGN'
		else 'EUR'
		END as currency,*/
	substring(area from '[\d]+')::bigint as area,
	CASE WHEN LOWER(TRIM(floor)) IN ('партер', 'сутерен') then 1 ELSE substring(floor from '[\d]+')::bigint END as floor,
	description, views::bigint as views, date, agency,
	'2020-06-25' as measurement_day
FROM holmes_import
						 

insert into holmes (link, title, address, details, region, place, lon, lat, id, price, price_sqm, area, floor, description, views, date, agency, measurement_day)
select * from holmes_import_casted

select measurement_day, count(*) from holmes group by 1 order by 1

----QA----
select * from (
select *, lower(details->>'Етаж:') as fl from holmes) a
where fl = 'партер' and floor is null
limit 10

----------
--- delete
DELETE FROM holmes
  WHERE measurement_day = '2020-06-25'