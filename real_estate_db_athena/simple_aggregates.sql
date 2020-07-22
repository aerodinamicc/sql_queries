
------------
-- Neighbourhood/site aggregates
------------
select 
url_extract_host(link),
trim(lower(place)), 
count(*) ,
approx_percentile(round(cast(replace(price, ' ') as double) / cast(replace(area, ' ') as double)), 0.5) as median_price,
round(avg(round(cast(replace(price, ' ') as double) / cast(replace(area, ' ') as double)))) as average_price
from real_estate_db.daily_measurements 
where url_extract_host(link) not in ('etuovi.com', 'www.vuokraovi.com', 'sofia.holmes.bg')
and price <> '0' 
and area <> '0'
and try_cast(replace(price, ' ') as double) is not null
and try_cast(replace(area, ' ') as double) is not null 
group by 1, 2
order by 4 desc

-----------
-- Finnish offers by city and neighbourhood - Etuovi
-----------
select *
from (
	select 
		trim(lower(type)) as type, 
		city, 
		case when cardinality(split(place, ', ')) = 3 then trim(lower(replace(split(place, ', ')[2], ','))) else '' end as nbhd, 
		round(avg(cast(price as double)/cast(area as double))) as average_price,
		round(approx_percentile(cast(price as double)/cast(area as double), 0.5)) as median_price,
		min(cast(price as double)) as min_price,
		max(cast(price as double)) as max_price,
		count(*) as rows
	from real_estate_db.daily_measurements 
	where url_extract_host(link) in ('etuovi.com')
	and price <> '0' 
	and area <> '0'
	and try_cast(replace(price, ' ') as double) is not null
	and try_cast(replace(area, ' ') as double) is not null 
	and city = 'espoo'
	and cast(price as double) > 50000
	group by 1, 2, 3
	order by 4 desc
)
where --nbhd in ('matinkylä', 'olari', 'leppävaara', 'etelä-leppävaara', 'kivenlahti', 'säteri', 'pohjois-tapiola', 'tapiola', 'westend', 'finnoo')
--and 
type = 'kerrostalo'
and rows > 10

-------------
---- Finnish standart scores
-------------
select 
	f.link,
	--type,
	city,
	place,
	price_sqm,
	mean_price_sqm,
	price,
	round(try_cast(et.monthly_fee as double) * 12 * 10) fees_for_10_yrs,
	price + round(try_cast(et.monthly_fee as double) * 12 * 10) as price_after_10_yrs,
	area,
	price_sqm_anomaly,
	perc_price_sqm_diff_than_mean,
	et.monthly_fee,
	et.maintainance_fee,
	et.financial_fee,
	et.floor,
	et.communications
from (
	select *,
		round(case when deviation_price_sqm <> 0 then deviation_price_sqm/std_price_sqm else 0 end, 2) as price_sqm_anomaly,
		ROUND(((price_sqm - mean_price_sqm) * 100) / mean_price_sqm, 2) as perc_price_sqm_diff_than_mean
	from (
		select 
			*,
			round(AVG(price_sqm) over (partition by city, type, place)) as mean_price_sqm,
			price_sqm - AVG(price_sqm) over (partition by city, type, place) as deviation_price_sqm,	
			stddev(price_sqm) over (partition by city, type, place) as std_price_sqm,
			COUNT(*) over (partition by city, type, place) as quarter_rows
		from (
			select 
				regexp_extract(link, '(.*)\?', 1) as link,
				type,
				city,
				case when cardinality(split(place, ', ')) = 3 then trim(lower(replace(split(place, ', ')[2], ','))) else '' end as place,
				round(cast(replace(price, ' ') as double) / cast(replace(area, ' ') as double)) as price_sqm,
				cast(replace(price, ' ') as double) as price,
				cast(replace(area, ' ') as double) as area
			from real_estate_db.daily_measurements 
			where url_extract_host(link) in ('etuovi.com')
			and price <> '0' 
			and area <> '0'
			and try_cast(replace(price, ' ') as double) is not null
			and try_cast(replace(area, ' ') as double) is not null 
		)
		where lower(trim(type)) = 'kerrostalo'
	)
	where quarter_rows > 10
) as f
left join real_estate_db.etuovi_details et using(link)
where perc_price_sqm_diff_than_mean < -20
--and perc_price_sqm_diff_than_mean > -15
--and price < 200000
and price > 100000
and price_sqm < 3000
and area > 45
and city in ('espoo', 'vantaa', 'helsinki')
and place not in ('viherlaakso', 'suurpelto', '')
and try_cast(et.monthly_fee as double) < 400
--and (price + round(try_cast(et.monthly_fee as double) * 12 * 10) < 250000 or price < 200000)
and perc_price_sqm_diff_than_mean < -30
order by 8



-------------
--- Entries with certain levels of deviation
-------------
select 
	link,
	type,
	place,
	price_sqm,
	mean_price_sqm,
	price,
	area,
	price_sqm_anomaly,
	perc_price_sqm_diff_than_mean
from (
	select *,
		round(case when deviation_price_sqm <> 0 then deviation_price_sqm/std_price_sqm else 0 end, 2) as price_sqm_anomaly,
		ROUND(((price_sqm - mean_price_sqm) * 100) / mean_price_sqm, 2) as perc_price_sqm_diff_than_mean
	from (
		select 
			*,
			round(AVG(price_sqm) over (partition by url_extract_host(link), type, place)) as mean_price_sqm,
			price_sqm - AVG(price_sqm) over (partition by url_extract_host(link), type, place) as deviation_price_sqm,	
			stddev(price_sqm) over (partition by url_extract_host(link), type, place) as std_price_sqm,
			COUNT(*) over (partition by url_extract_host(link), type, place) as quarter_rows
		from (
			select 
				link,
				type,
				trim(lower(place)) as place,
				round(cast(replace(price, ' ') as double) / cast(replace(area, ' ') as double)) as price_sqm,
				cast(replace(price, ' ') as double) as price,
				cast(replace(area, ' ') as double) as area
			from real_estate_db.daily_measurements 
			where url_extract_host(link) not in ('etuovi.com', 'www.vuokraovi.com', 'sofia.holmes.bg')
			and price <> '0' 
			and area <> '0'
			and try_cast(replace(price, ' ') as double) is not null
			and try_cast(replace(area, ' ') as double) is not null 
			and cast(is_for_sale as boolean)
		)
	)
	where quarter_rows > 10
)
where perc_price_sqm_diff_than_mean < -10
--and perc_price_sqm_diff_than_mean > -15
and price < 80000
and regexp_like(lower(place), 'дружба')
order by 9

----------------------
--- Random

select *
from real_estate_db.daily_measurements 
where url_extract_host(link) in ('yavlena.com')
limit 10

select *
from real_estate_db.daily_measurements 
where type = 'Парцел в регулация'


-------------------------
---- Reduced price

with day_rnk as (
select measurement_day, row_number() over (order by measurement_day desc) as rnk
from real_estate_db.daily 
group by 1
),
latest as (
select 
	site, 
	id,
	place,
	type,
	price,
	area,
	link
from real_estate_db.daily 
where measurement_day = (select measurement_day from day_rnk where rnk = 1)
and url_extract_host(link) not in ('etuovi.com', 'www.vuokraovi.com') 
),
prev as (
select 
	site, 
	id,
	type,
	price,
	area
from real_estate_db.daily 
where measurement_day = (select measurement_day from day_rnk where rnk = 2)
and country = 'bg'
and is_for_sale
),
median_rent as (
select 
	place,
	approx_percentile(price, 0.5) as avg_rent_price
from real_estate_db.daily 
where site in ('yavlena.com', 'imoteka.bg')
and not is_for_sale
and country = 'bg'
and price < 10000
group by 1
)
select
	latest.site,
	latest.id,
	latest.place,
	latest.type,
	prev.price as prev_price,
	latest.price as latest_price,
	latest.price - prev.price as price_diff, 
	median_rent.avg_rent_price,
	latest.area,
	latest.link
from latest
inner join prev on 
	latest.site = prev.site 
	and latest.id = prev.id 
	and latest.type = prev.type 
	and latest.area = prev.area
	and latest.price < prev.price
left join median_rent using(place)
where latest.price < 60000


------------------------------
-- Check how many price rows are convertible to float

select
	url_extract_host(link) as site,
	is_for_sale,
	sum(case when price = '0' then 1 else 0 end) as zeros,
	sum(case when price = '' then 1 else 0 end) as emptys,
	sum(case when try_cast(replace(price, ' ') as double) is not null 
				and try_cast(replace(price, ' ') as double) > 0 then 1 else 0 end) as is_converted_well,
	sum(case when try_cast(replace(price, ' ') as double) is not null 
				and try_cast(replace(price, ' ') as double) < 1 then 1 else 0 end) as is_converted_zero,
	sum(case when try_cast(replace(price, ' ') as double) is null then 1 else 0 end) as is_converted_badly,
	count(*)
from real_estate_db.daily_measurements
where measurement_day = '2020-07-19'
group by 1, 2

------
--Rents

select 
	site,
	count(*)
from real_estate_db.daily
where not is_for_sale 
and measurement_day = date'2020-07-20'
group by 1


select 
	*
from real_estate_db.daily 
where site in ('yavlena.com', 'imoteka.bg')
and not is_for_sale
and country = 'bg'
and price < 10000
and place = 'хаджи димитър'
limit 200



