---------------	 
-- Get parcels
---------------

CREATE TYPE parcels_type AS (id text, title text, address text, details JSON, place text, region text, lon float, lat float, price float, price_sqm float, 
										  area BIGINT, floor BIGINT, views bigint, measurement_day text, agency text, date text, description text, link text);

CREATE FUNCTION get_parcels(is_regulation varchar(10), is_water varchar(10), is_electricity varchar(10), below_area bigint, below_price bigint) RETURNS SETOF parcels_type
AS $$
select id, title, address, details, place, region, lon, lat, price, price_sqm, area, floor, views, measurement_day, agency, date, description, link
from (
select 
	trim(details->>'Регулация:') as regulation,
	trim(details->>'Вода:') as water,
	trim(details->>'Ток:') as electricity,
	*
from imoti
where measurement_day = (select max(measurement_day) from imoti)
and title = 'ПАРЦЕЛ'
and lon < 42.6266 --Ring road
and area < below_area
and price < below_price
and details->>'Регулация:' is not null
and details->>'Вода:' is not null
and details->>'Ток:' is not null
) а
where regulation like is_regulation and water like is_water and electricity like is_electricity
order by price 
$$
LANGUAGE SQL;

select * from get_parcels('ДА','ДА','ДА', 1000, 50000)


--------
--- Agg stats
--------

select 
	place,
	count(distinct(id))  as properties,
	percentile_disc(0.5) within group (order by price) as median_price,
    round(avg(price), 2) as avg_price,
	percentile_disc(0.5) within group (order by price_sqm) as median_price_sqm,
    round(avg(price_sqm), 2) as avg_price_sqm
from imoti
where measurement_day = (select max(measurement_day) from imoti)
and title = 'ПАРЦЕЛ'
and lon < 42.6266 --Ring road
group by 1
order by 5 desc
