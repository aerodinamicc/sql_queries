---------------	 
-- Get those properies that are reduced in price
---------------
/*
On a side note there's normally a distinction within other DBMS that functions can only call SELECT statements and should not modify data 
while procedures should be handling the data manipulation and data definition languages (DML, DDL). 
*/
drop type if exists reduced_properties_result

CREATE TYPE reduced_properties_result AS (id text, title text, address text, details JSON, place text, lon float, lat float, price float, price_sqm float, 
										  area BIGINT, floor BIGINT,  measurement_day text, place_avg_price float, delta_from_avg_price float, price_diff float, 
										 price_diff_percentage float, link text, description text);

										 
drop function if exists most_reduced
										 
CREATE FUNCTION most_reduced(type_of_property varchar(30), min_diff bigint, price_less_than bigint) RETURNS SETOF reduced_properties_result
AS $$
with agg as (
select *
from (
	select 
		id,
		last_value(price) over (partition by id order by measurement_day) - lag(price) over (partition by id order by measurement_day) as price_diff,
		row_number() over (partition by id order by measurement_day desc) as rnk
	from holmes
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
	ROUND(AVG(price/area) over (partition by place)) as place_avg_price,
	ROUND((price/area) - AVG(price/area) over (partition by place)) as delta_from_avg_price,
	agg.price_diff,
	round((agg.price_diff * 100. / (price + ABS(agg.price_diff)))::float, 2)::float as price_diff_percentage,
	link,
	description
from holmes
left join (select distinct id, price_diff from agg) agg using(id)
where price > 0 and price < price_less_than
and agg.price_diff < min_diff
and holmes.measurement_day = (select max(measurement_day) from holmes)
and title ~ type_of_property
order by agg.price_diff asc, id $$
LANGUAGE SQL;

select * from most_reduced('АПАРТАМЕНТ', -1000, 35000)

-------------------
--- daily
-------------------

CREATE TYPE reduced_properties_daily AS (place text, price float, old_price float, price_diff float, area float, type text, link text);

										 
drop function if exists most_reduced_daily
										 
CREATE FUNCTION most_reduced_daily(min_diff bigint, price_less_than bigint) RETURNS SETOF reduced_properties_daily
AS $$
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
	where rnk in (1, 2)
),
entries as (
	select 
		place, price, area, type, link, measurement_day 
	from daily_measurements dm 
	left join daily_metadata meta 
	on meta.site = dm.site and meta.id = dm.id
	where measurement_day in (select measurement_day from dates)
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
where ent2.measurement_day is not null
and ent.price - ent2.price < min_diff
and ent.price < price_less_than
order by 4 $$
LANGUAGE SQL;

select * from most_reduced_daily(-1000, 100000)