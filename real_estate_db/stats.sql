-----
-- Most recent date - aggregate scores
-----

select 
	place, 
	count(*) as rows,
	percentile_disc(0.5) within group (order by price) as median_price,
    round(avg(price), 2) as avg_price,
    stddev(price) as std_price,
	percentile_disc(0.25) within group (order by price_sqm) as percentile_25_price,
	percentile_disc(0.5) within group (order by price_sqm) as median_price_sqm,
    round(avg(price_sqm), 2) as avg_price_sqm,
    stddev(price_sqm) as std_price_sqm 
from imoti
where measurement_day = (select max(measurement_day) from imoti)
and lower(imoti.title) ~ 'апартамент'
group by 1
order by 6 desc

-----
-- New and closed offers
-----

select
	measurement_day,
	count(*) as rows,
	SUM(case when first_time <> 1 and last_time <> 1 then 1 else 0 end) as continued_offers,
	SUM(case when first_time = 1 and last_time <> 1 then 1 else 0 end) as first_timers,
	SUM(case when first_time = 1 and last_time = 1 then 1 else 0 end) as first_and_last_timers,
	SUM(case when first_time <> 1 and last_time = 1 then 1 else 0 end) as last_timers,
	SUM(case when prev_price is not NULL and prev_price > price then 1 else 0 end) as offers_cheaper,
	SUM(case when prev_price is not NULL and prev_price = price then 1 else 0 end) as offers_same_price,
	SUM(case when prev_price is not NULL and prev_price < price then 1 else 0 end) as offers_more_expensive
from (	
	select 
		link, 
		measurement_day, 
		price,
		LAG(price) over (partition by link
							order by measurement_day asc) as prev_price,
		row_number() over (partition by link
							order by measurement_day asc) as first_time,
		row_number() over (partition by link
							order by measurement_day desc) as last_time
	from imoti
	order by 1, 2
	) i
group by 1
order by 1

/*
day			rows	cont-ed	first	first	last	offers		offers	offers
					_offers	_timers	_and	_timers	_cheaper	_same	_more
									_last						_price	_expensive	
									_timers
2020-03-21	27,536	0		24,197	3,339	0		0			0		0
2020-04-05	26,385	19,469	3,448	1,704	1,764	628			20,475	130
2020-04-25	26,102	20,989	2,263	1,242	1,608	815			21,561	221
2020-05-09	26,161	19,949	1,743	1,423	3,046	726			22,004	265
2020-05-29	23,479	17,898	3,407	992		1,182	1,149		17,454	477
2020-06-05	28,216	20,018	1,399	1,484	5,315	755			24,247	331
2020-06-25	28,063	0		0		4,521	23,542	1,338		21,594	610
*/

-------------
-- Standart score against neighbourhood aggregates
-------------
with src as (
select 
	link,
	id, 
	lon,
	lat,
	title, 
	place, 
	price, 
	price_sqm, 
	area,
	round(AVG(price_sqm) over (partition by title, place), 2) as mean,
	round(price_sqm - AVG(price_sqm) over (partition by title, place), 2) as dev,
	round(stddev(price_sqm) over (partition by title, place), 2) as std
from imoti
where measurement_day = (select max(measurement_day) from imoti)
),
sc as (
select 
	*,
	round(case when dev <> 0 then dev/std else 0 end, 2) as price_sqm_anomaly
from src
)
select 
	*
from sc 
where standart_score < -2
and lon is not null
and lat is not null
and price < 80000
and lower(imoti.title) ~ 'апартамент'


------------------------------
------- Removed offer ids since last time
------------------------------
with date_rnk as (
select 
	measurement_day,
	row_number() over (order by measurement_day desc) as rnk
from (
select
	distinct measurement_day
from imoti
) i
),
ranked as (
select
	distinct link, id, lon, lat, title, place, price, price_sqm, measurement_day, date_rnk.rnk
from imoti
left join date_rnk using(measurement_day)
)
select 
	place, title, COUNT(distinct(id)) as rows
from ranked
where measurement_day = (select measurement_day from date_rnk where rnk = 2)
WHERE lower(imoti.title) ~ 'апартамент'
and not id in (select distinct id from ranked where rnk = 1)
group by 1, 2
order by 1, 2

-----------------
--- Get offers with standart scores
-----------------

with medians as (
select 
	place, 
	title,
	percentile_disc(0.5) within group (order by price_sqm) as median_price_sqm,
	percentile_disc(0.5) within group (order by price) as median_price
from imoti
where measurement_day = (select max(measurement_day) from imoti)
and lower(title) ~ 'апартамент'
group by 1, 2
),
stats as (
select 
	link, title, place, price, price_sqm, area,
	medians.median_price_sqm,
	ROUND(avg(price_sqm) over (partition by place, title), 2) as mean_price_sqm,
	ROUND(stddev(price_sqm) over (partition by place, title), 2) as std_price_sqm,
	round(price_sqm - AVG(price_sqm) over (partition by title, place), 2) as dev_price_sqm,
	round(medians.median_price, 2) as median_price,
	ROUND(avg(price) over (partition by place, title), 2) as mean_price,
	round(stddev(price) over (partition by place, title), 2) as std_price,
	round(price - AVG(price) over (partition by title, place), 2) as dev_price	
from imoti
left join medians using(place, title)
WHERE lower(imoti.title) ~ 'апартамент'
)
select 
	place,
	title,
	price,
	round(price_sqm) as price_sqm,
	area,
	round(case when dev_price_sqm <> 0 then dev_price_sqm/std_price_sqm else 0 end, 2) as price_sqm_anomaly,
	ROUND(((price_sqm - mean_price_sqm) * 100) / mean_price_sqm, 2) as perc_price_sqm_diff_than_mean,
	mean_price,
	round(case when dev_price <> 0 then dev_price/std_price else 0 end, 2) as price_anomaly,
	ROUND(((price - mean_price) * 100) / mean_price, 2) as perc_price_diff_than_mean,
	link
from stats
where lower(place) ~ 'младост 4'

with holmes as (
select place, count(*) as rows, avg(price_sqm) as avg_price_sqm
from holmes
where measurement_day = (select max(measurement_day) from holmes)
and lower(title) ~ 'апартамент'
group by 1
order by 3 desc
),
yavlena as (
select place, count(*) as rows, avg(price_sqm) as avg_price_sqm
from yavlena
where is_for_sale
and lower(type) ~ 'стаен'
group by 1
order by 3 desc
)
select 
	*
from holmes
left join yavlena using(place)

------
-- imotibg
------
select place, count(*), avg(price_sqm) 
from imotibg 
where price_sqm > 1
and lower(title) ~ 'апартамент'
group by 1
order by 3 desc

--------------
-- check yavlena in holmes records
--------------
select 
agency, count(*)
from holmes
where measurement_day = (select max(measurement_day) from holmes)
and agency ~ 'ЯВЛЕНА'
group by 1
order by 2 desc



	
