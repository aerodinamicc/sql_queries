---------------	 
-- Get those properies that are reduced in price
---------------
/*
On a side note there's normally a distinction within other DBMS that functions can only call SELECT statements and should not modify data 
while procedures should be handling the data manipulation and data definition languages (DML, DDL). 
*/
CREATE TYPE reduced_properties_result AS (id text, title text, address text, details JSON, place text, lon float, lat float, price float, price_sqm float, 
										  area BIGINT, floor BIGINT,  measurement_day text, price_diff float, price_diff_percentage float, link text);

CREATE FUNCTION most_reduced(type_of_property varchar(30), min_diff bigint, price_less_than bigint) RETURNS SETOF reduced_properties_result
AS $$
with agg as (
select *
from (
	select 
		id,
		last_value(price) over (partition by id order by measurement_day) - max_price.max_price as price_diff,
		row_number() over (partition by id order by measurement_day desc) as rnk
	from holmes
		left join (select id, max(price) as max_price from holmes group by 1) max_price using(id)
	) agg
WHERE rnk = 1
)
select
	id,
	title,
	address,
	details,
	place,
	lon, lat,
	price, price_sqm, area, floor, 
	measurement_day,
	agg.price_diff,
	round((agg.price_diff * 100. / (price + ABS(agg.price_diff)))::float, 2)::float as price_diff_percentage,
	link
from holmes
left join (select distinct id, price_diff from agg) agg using(id)
where agg.price_diff < min_diff
and holmes.measurement_day = (select max(measurement_day) from holmes)
and title ~ type_of_property
and price < price_less_than
order by agg.price_diff asc, id $$
LANGUAGE SQL;

select * from most_reduced('АПАРТАМЕНТ', -1000, 35000)