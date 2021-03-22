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
	measurement_day VARCHAR
);

CREATE FUNCTION ROUND(float,int) RETURNS NUMERIC AS $$
   SELECT ROUND($1::numeric,$2);
$$ language SQL IMMUTABLE;

drop table if exists holmes

CREATE TABLE holmes
(
	LINK varchar, 
	TITLE varchar,
	ADDRESS varchar, 
	DETAILS json, 
	PLACE varchar, 
	LON float, 
	LAT float, 
	ID varchar, 
	PRICE float, 
	PRICE_SQM float, 
	AREA bigint, 
	FLOOR bigint,
	DESCRIPTION varchar, 
	VIEWS bigint, 
	DATE varchar,
	measurement_day varchar
);


CREATE TABLE holmes_import_casted AS
SELECT 
	link, 
    title, 
	substring(address from trim(place)||'(.*)') as address, 
	replace(substring(details, 2, length(details)-2), '""', '"')::json,
	substring(address from '(.*)'||trim(place)) as region, 
	trim(place), 
    lon::float, 
    lat::float,
	id, 
	case when lower(price) like '%лв%' THEN round(replace(substring(price from '[\d\s]+'), ' ', '')::float / 1.9588, 2)
		 WHEN trim(price) = 'при запитване' THEN 0.
		 ELSE replace(substring(trim(price) from '[\d\s]+'), ' ', '')::FLOAT
		END as price,
	case when price_sqm like '%лв%' then round(substring(price_sqm from '[\d\.]+')::float / 1.9588, 2)
		 WHEN trim(price) = 'при запитване' THEN 0.
		 ELSE substring(price_sqm from '[\d\.]+')::FLOAT 
		END as price_sqm,
	substring(area from '[\d]+')::bigint as area,
	CASE WHEN LOWER(TRIM(floor)) IN ('партер', 'сутерен') then 1 ELSE substring(floor from '[\d]+')::bigint END as floor,
	description, 
    views::bigint, 
    date,
	measurement_day
FROM holmes_import

select 
	place, title, price, price_sqm, area, price_diff, price_diff_percentage, details, link
from most_reduced('апартамент$', -10000, 90000)
where price > 0



CREATE TABLE daily_real_estate (
	link VARCHAR,
	id VARCHAR,
	type VARCHAR,
	city VARCHAR,
	place VARCHAR,
	is_for_sale BOOLEAN,
	is_apartment BOOLEAN,
	price FLOAT,
	area FLOAT,
	details VARCHAR,
	labels VARCHAR, 
	year FLOAT,
	available_from VARCHAR,
	views FLOAT,
	lon FLOAT,
	lat FLOAT,
	measurement_day VARCHAR
);

drop table if exists daily_real_estate

drop table if exists daily_import

CREATE TABLE if not exists daily_import (
	link VARCHAR,
	is_for_sale VARCHAR,
	price VARCHAR,
	labels VARCHAR, 
	views VARCHAR,
	measurement_day VARCHAR,
	country VARCHAR,
	id VARCHAR,
	type VARCHAR,
	city VARCHAR,
	place VARCHAR,
	is_apartment VARCHAR,
	area VARCHAR,
	details VARCHAR,
	year VARCHAR,
	available_from VARCHAR,
	lon VARCHAR,
	lat VARCHAR
);

select * from daily_import_casted limit 10

create function try_cast_float(p_in text, p_default null)
   returns int
as
$$
begin
  begin
    return $1::float;
  exception 
    when others then
       return p_default;
  end;
end;
$$
language plpgsql;


create table daily_import_casted as (
select
link,country,id,type,city,place,
is_for_sale::boolean,
is_apartment::boolean,
price::float,
area::float,
replace(substring(details, 2, length(details)-2), '""', '"')::json as details,
labels,
year::float,
available_from,
views::FLOAT,
lon::FLOAT,
lat::FLOAT,
measurement_day
)


drop table if exists daily_metadata

CREATE TABLE if not exists daily_metadata (
	link VARCHAR,
	country VARCHAR,
	id VARCHAR unique,
	type VARCHAR,
	city VARCHAR,
	place VARCHAR,
	is_apartment BOOLEAN,
	area FLOAT,
	details JSON,
	year FLOAT,
	available_from VARCHAR,
	lon FLOAT,
	lat FLOAT
);

CREATE TABLE if not exists daily_measurements (
	id VARCHAR,
	is_for_sale BOOLEAN,
	price FLOAT,
	labels VARCHAR, 
	views FLOAT,
	measurement_day VARCHAR
);

insert into holmes (link, id, type, city, place, area, details, year, available_from, lon, lat)
select * from daily_metadata

insert into holmes (id, is_for_sale, price, labels, views, measurement_day)
select * from daily_measurements

select * from daily_import di limit 10

-----
--Reduced example
-----

with dates as (
	select measurement_day, rnk
	from 
	(
		select 
			measurement_day, 
			row_number() over (order by measurement_day desc) as rnk
		from 
		(
			select measurement_day from daily_measurements group by 1
		) foo
	) foo1
	where rnk in (select * from generate_series(1, 20))
),
entries as (
	select 
		distinct place, price, area, type, link, dm.measurement_day 
	from daily_measurements dm 
	left join daily_metadata meta 
	on meta.site = dm.site and meta.id = dm.id
	left join dates
	on dates.measurement_day = dm.measurement_day
	where dm.measurement_day in (select measurement_day from dates)
	and is_apartment
	and is_for_sale
	and dm.site not like 'address.bg'
)
select 
	ent.place,
	ent.price,
	ent2.price as old_price,
	ent.price - ent2.price as price_diff,
	ent.area,
	ent.type,
	ent.link
from entries ent
left join entries ent2
on ent.link = ent2.link 
and ent.measurement_day > ent2.measurement_day
where ent2.measurement_day <= (select measurement_day from dates where rnk = 10)
and ent.measurement_day = (select measurement_day from dates where rnk = 1)
and ent.price - ent2.price < -1000
order by 4



