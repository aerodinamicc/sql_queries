---------------	 
-- Get most recent properties
---------------

CREATE TYPE get_prop_from_date_type AS (id text, title text, address text, details JSON, place text, region text, lon float, lat float, price float, price_sqm float, 
										  area BIGINT, floor BIGINT, views bigint, measurement_day text, agency text, date text, description text, link text);

CREATE FUNCTION get_prop_from_date(date_of_interest varchar(10)) RETURNS SETOF get_prop_from_date_type
AS $$
select id, title, address, details, place, region, lon, lat, price, price_sqm, area, floor, views, measurement_day, agency, date, description, link
from imoti
where measurement_day = date_of_interest $$
LANGUAGE SQL;

select measurement_day, count(*) as rows from get_prop_from_date('2020-05-09') group by 1