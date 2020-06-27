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
and lower(title) like '%апартамент%'
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
-- Individual offers against neighbourhood aggregates
-------------

select 
	id, 
	title, 
	place, 
	price, 
	price_sqm, 
	area,
	AVG(price) over (partition by title, place 
					order by )
from imoti
where measurement_day = (select max(measurement_day) from imoti)
limit 10


select * from imoti limit 10

select distinct(poly) from imoti
