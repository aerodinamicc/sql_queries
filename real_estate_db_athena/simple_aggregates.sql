
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
	and city = 'vantaa'
	and cast(price as double) > 50000
	group by 1, 2, 3
	order by 4 desc
)
where --nbhd in ('matinkylä', 'olari', 'leppävaara', 'etelä-leppävaara', 'kivenlahti', 'säteri', 'pohjois-tapiola', 'tapiola', 'westend', 'finnoo')
--and 
type = 'kerrostalo'
and rows > 10

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
and price < 70000
order by 1, 9


select *
from real_estate_db.daily_measurements 
where url_extract_host(link) in ('yavlena.com')
limit 10

select *
from real_estate_db.daily_measurements 
where type = 'Парцел в регулация'


