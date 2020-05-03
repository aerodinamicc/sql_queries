--------
-- CREATE MASTER TABLE
--------
drop table if exists  imoti

CREATE TABLE imoti
(
	LINK varchar, 
	TITLE varchar,
	ADDRESS varchar, 
	DETAILS json, 
	NEIGHBORHOOD varchar, 
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

CREATE TABLE imoti_import
(
	LINK varchar, 
	TITLE varchar,
	ADDRESS varchar, 
	DETAILS varchar, 
	NEIGHBORHOOD varchar, 
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

copy imoti_import (link, title, address, details, neighborhood, lon, lat, id, price, price_sqm, area, floor, description, views, date, agency, poly) 
FROM 'E:\imoti\holmes_2103_2020.tsv' DELIMITER E'\t' CSV HEADER


CREATE FUNCTION ROUND(float,int) RETURNS NUMERIC AS $$
   SELECT ROUND($1::numeric,$2);
$$ language SQL IMMUTABLE;


CREATE TABLE imoti_import_casted AS
SELECT 
	link, title, 
	substring(address from neighborhood||'(.*)') as address, 
	details::json,
	neighborhood, lon::float, lat::float,
	id, 
	case when lower(price) like '%лв%' then round(replace(substring(price from '[\d\s]+'), ' ', '')::float / 1.9588, 2)
		else replace(substring(trim(price) from '[\d\s]+'), ' ', '')::float 
		END as price, 
	case when price_sqm like '%лв%' then round(substring(price_sqm from '[\d\.]+')::float / 1.9588, 2)
		else substring(price_sqm from '[\d\.]+')::float 
		END as price_sqm,
	case when price like '%лв%' then 'BGN'
		else 'EUR'
		END as currency,
	substring(area from '[\d]+')::bigint as area,
	substring(floor from '[\d]+')::bigint as floor,
	description, views::bigint as views, date, agency,
	'2020-03-21' as measurement_day
FROM imoti_import

insert into imoti (link, title, address, details, neighborhood, lon, lat, id, price, price_sqm, currency, area, floor, description, views, date, agency, measurement_day)
select * from imoti_import_casted

select measurement_day, count(*) from imoti group by 1 order by 1

select * from imoti limit 10

drop table if exists imoti_import_casted
drop table if exists imoti_import

-----------------------------------------------
----------------- Exploration
-----------------------------------------------


---------
--- Apartments
---------
with agg as (
select 
	id,
	last_value(price) over (partition by id order by measurement_day) - max_price.max_price as price_diff
from imoti
	left join (select id, max(price) as max_price from imoti group by 1) max_price using(id)
)
select
	id,
	title,
	address,
	details,
	neighborhood,
	price, price_sqm, area, floor, 
	measurement_day,
	agg.price_diff,
	link
from imoti
left join (select distinct id, price_diff from agg) agg using(id)
where agg.price_diff < -5000
and measurement_day = '2020-04-25'
and title ~ 'АПАРТАМЕНТ'
and area > 60 and area < 80
and price < 80000
order by agg.price_diff asc, id


select * from imoti
WHERE id = '1b158279354601090'
order by measurement_day asc

--------------
--
--------------

with agg as (
select 
	id,
	last_value(price) over (partition by id order by measurement_day) - max_price.max_price as price_diff
from imoti
	left join (select id, max(price) as max_price from imoti group by 1) max_price using(id)
)
select
	id,
	title,
	address,
	details,
	neighborhood,
	price, price_sqm, area, floor, 
	measurement_day,
	agg.price_diff,
	link
from imoti
left join (select distinct id, price_diff from agg) agg using(id)
where agg.price_diff < -5000
and measurement_day = (select max(measurement_day) from imoti)
and title ~ 'ПАРЦЕЛ'
and price < 80000
order by agg.price_diff asc, id

----------------
/*
On a side note there's normally a distinction within other DBMS that functions can only call SELECT statements and should not modify data 
while procedures should be handling the data manipulation and data definition languages (DML, DDL). 
*/
CREATE TYPE reduced_properties_result AS (id text, title text, address text, details JSON, neighborhood text, price BIGINT, price_sqm float, 
										  area BIGINT, floor BIGINT,  measurement_day text, price_diff BIGINT, link text);

CREATE FUNCTION most_reduced(type_of_property varchar(30), min_diff bigint, price_less_than bigint) RETURNS SETOF reduced_properties_result
AS $$
with agg as (
select 
	id,
	last_value(price) over (partition by id order by measurement_day) - max_price.max_price as price_diff
from imoti
	left join (select id, max(price) as max_price from imoti group by 1) max_price using(id)
)
select
	id,
	title,
	address,
	details,
	neighborhood,
	price, price_sqm, area, floor, 
	measurement_day,
	agg.price_diff,
	link
from imoti
left join (select distinct id, price_diff from agg) agg using(id)
where agg.price_diff < min_diff
and measurement_day = (select max(measurement_day) from imoti)
and title ~ type_of_property
and price < price_less_than
order by agg.price_diff asc, id $$
LANGUAGE SQL;

select * from most_reduced('АПАРТАМЕНТ', -5000, 80000)



