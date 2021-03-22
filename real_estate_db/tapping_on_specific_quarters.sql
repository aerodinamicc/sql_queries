-----
-- All mladost apartments
-----

select 
distinct
	place, 
	type, 
	price, 
	area, 
	round(price / area) as price_sqm, 
	round(AVG(price / area) over (partition by place, type)) as avg_price_sqm,
	round(price / area) - round(AVG(price / area) over (partition by place)) as diff,
	link
from daily_measurements dm
inner join daily_metadata dmeta
on dm.site = dmeta.site
	and dm.id = dmeta.id
where measurement_day = (select max(measurement_day) from daily_measurements)
and is_for_sale 
and is_apartment
and place ~ '^младост'
and price < 110000
order by 7 asc

-------
-- Reduced apartments
-------

select 
	place, title, price, area, price_sqm, price_diff, price_diff_percentage, delta_from_avg_price, place_avg_price, lower(description) ~ 'ддс' as dds_mention, details -> 'Етаж' as floot, details, link 
from most_reduced('апартамент', -5000, 100000) mr
where price > 0
and lower(place) ~ 'изгрев|^младост|^изток|слатина|дианабад|лозенец|яворов|мусагеница|дървеница'
and title not like 'едностаен апартамент'

select
	place,
	title,
	price, price_sqm, area, floor, 
	measurement_day,
	ROUND(AVG(price/area) over (partition by place)) as place_avg_price,
	ROUND((price/area) - AVG(price/area) over (partition by place)) as delta_from_avg_price,
	details,
	link,
	description
from holmes
where  holmes.measurement_day = (select max(measurement_day) from holmes)

-------
-- Floor and price
-------

select
	place,
	title,
	case when substring(details ->> 'Етаж' from '([\d]+)') = '1' then '1'
		when cast(substring(details ->> 'Етаж' from '([\d]+)') as INTEGER) > 7 then '7+'
		when substring(details ->> 'Етаж' from '([\d]+)') is null then null
		else '2-7' end as floor,
	LOWER(description) ~ 'метро' as is_subway,
	ROUND(AVG(price_sqm)) as price_sqm,
	MIN(price_sqm) as min_price,
	MAX(price_sqm) as max_price,
	COUNT(*) as rows_
from holmes
where  holmes.measurement_day = (select max(measurement_day) from holmes)
and trim(lower(place)) ~ 'младост (?:1|2|3)$'
and title ~ ('апартамент$|мезонет|ателие')
group by 1, 2, 3, 4
having count(*) > 5


-------
-- Get subset by type regex and neighbourhood regex
-------
drop TYPE if exists estates_result

CREATE TYPE estates_result AS (place text, type text, floor float, construction text, is_subway boolean, area float, price float, price_sqm float, 
										 median_sqm float, delta_from_median_sqm float, 
										avg_sqm float, delta_from_avg_sqm float, link text);

										 
drop function if exists estates
										 
CREATE FUNCTION estates(property_regex varchar(1024), quarters_regex varchar(1024)) RETURNS SETOF estates_result
AS $$
with stats as (
select 
	place,
	ROUND(percentile_disc(0.5) within group (order by price/area)) as median_sqm
from holmes
group by 1
)
SELECT 
	lower(h.place) as place,
	lower(title) as type,
	cast(substring(details ->> 'Етаж' from '([\d]+)') as float) as floor,
	case when details::jsonb ? 'Особености' and details ->> 'Особености' ~ '(?i)тухла|епк' then 'тухла|епк'
		when details::jsonb ? 'Особености' and details ->> 'Особености' ~ '(?i)панел' then 'панел'
		else null end as construction,
	LOWER(description) ~ 'метро' as is_subway,
	area::float,
	price,
	round(price/area) AS price_sqm,
	s.median_sqm,
	round((price/area) - s.median_sqm) as delta_from_median_sqm,
	ROUND(AVG(price/area) over (partition by h.place)) as avg_sqm,
	ROUND((price/area) - AVG(price/area) over (partition by h.place)) as delta_from_avg_sqm,
	link
from holmes h
left join stats s on h.place = s.place
where measurement_day = (select max(measurement_day) from holmes)
and lower(h.place) ~ quarters_regex
and lower(title) ~ property_regex
$$
LANGUAGE SQL;

drop function if exists estates_neg;


CREATE FUNCTION estates_neg(property_regex varchar(1024), neg_quarters_regex varchar(1024)) RETURNS SETOF estates_result
AS $$
with stats as (
select 
	place,
	ROUND(percentile_disc(0.5) within group (order by price/area)) as median_sqm
from holmes
group by 1
)
SELECT 
	lower(h.place) as place,
	lower(title) as type,
	cast(substring(details ->> 'Етаж' from '([\d]+)') as float) as floor,
	LOWER(description) ~ 'метро' as is_subway,
	area::float,
	price,
	round(price/area) AS price_sqm,
	s.median_sqm,
	round((price/area) - s.median_sqm) as delta_from_median_sqm,
	ROUND(AVG(price/area) over (partition by h.place)) as avg_sqm,
	ROUND((price/area) - AVG(price/area) over (partition by h.place)) as delta_from_avg_sqm,
	link
from holmes h
left join stats s on h.place = s.place
where measurement_day = (select max(measurement_day) from holmes)
and not lower(h.place) ~ neg_quarters_regex
and lower(title) ~ property_regex
$$
LANGUAGE SQL;

select * from estates('апартамент$|мезонет|ателие', 'дружба')
where area >= 50 and area <=60
and floor > 2
--and lower(description) ~ 'техничес'
order by 6


--------- Парцел Бистрица
select * from estates('парцел', 'бистрица')
--where lower(description) ~ 'марина'
order by 6

-----Младост
select * 
from estates('апартамент$|мезонет|ателие', 'младост')
where floor > 2
and area >= 60 and area <= 70
and construction ~ 'тухла'
and price <= 80000
order by delta_from_avg_sqm

select * 
from estates('апартамент$|мезонет|ателие', 'дианабад')
where floor > 2
and area >= 50 and area <= 80
--and construction ~ 'тухла'
--and price <= 80000
order by delta_from_avg_sqm

'http://sofia.holmes.bg/pcgi/home.cgi?act=3&adv=1b151177879091777'

select * from daily_import limit 10

select title, area, place, price, price_sqm, price_diff, delta_from_avg_price, price_diff_percentage, link 
from most_reduced('апартамент', 1000, 100000)
where price_diff < -999
order by place, price

select 
	place, type, '', price * 1.03 as final_price, price, price_sqm, area, floor, case when floor > 2 then 'high' else 'low' end, link
from estates('апартамент$|мезонет|ателие', 'младост')
where link not in (
	'http://sofia.holmes.bg/pcgi/home.cgi?act=3&adv=1b159992513578462',
	'http://sofia.holmes.bg/pcgi/home.cgi?act=3&adv=1c160519113871247',
	'http://sofia.holmes.bg/pcgi/home.cgi?act=3&adv=1c160491306976585',
	'http://sofia.holmes.bg/pcgi/home.cgi?act=3&adv=1b159992513578462',
	'http://sofia.holmes.bg/pcgi/home.cgi?act=3&adv=1b160166653935415'
)

select max(measurement_day) from holmes

select 
	place, type, '', price * 1.03 as final_price, price, price_sqm, area, floor, case when floor > 2 then 'high' else 'low' end, construction,  link
from estates('апартамент$|мезонет|ателие', 'младост 2')
where price = 95000
--where price_sqm <= 1300 and price_sqm >= 1150
and price < 100000
and area >= 50
/*and link in ('https://sofia.holmes.bg/pcgi/home.cgi?act=3&adv=1b160694589803301'
'https://sofia.holmes.bg/pcgi/home.cgi?act=3&adv=1b159014148731753',
	'https://sofia.holmes.bg/pcgi/home.cgi?act=3&adv=1b155370807099723',
	'https://sofia.holmes.bg/pcgi/home.cgi?act=3&adv=1c160637675666132',
	'https://sofia.holmes.bg/pcgi/home.cgi?act=3&adv=1b158618688594135',
	'https://sofia.holmes.bg/pcgi/home.cgi?act=3&adv=1b160386740722502',
	)*/
order by price_sqm desc

select 
	place, 
	type, 
	construction, 
	case when floor > 3 then True when floor < 4 then FALSE else null end as is_high,
	count(*) as rows_,
	round(avg(price/area)) as avg_price_sqm,
	round(min(price/area)) as min_price_sqm,
	round(max(price/area)) as max_price_sqm,
	round(min(price)) as min_price,
	round(max(price)) as max_price
from estates('апартамент$|мезонет|ателие', 'дианабад')
group by 1, 2, 3, 4
order by 1, 2, 3, 4

select *
from estates('апартамент$|мезонет|ателие', 'дианабад')
where type = 'тристаен апартамент'
and floor > 3
and construction = 'тухла|епк'
and round(price/area) < 1500



select * from holmes
where measurement_day = (select max(measurement_day) from holmes)
and description ~ '(?i)new comfort'


create table tenders (
	offer_id INT,
	published TEXT,
	title TEXT,
	area FLOAT,
	price FLOAT,
	city TEXT,
	address TEXT,
	organizer TEXT,
	term_start TEXT,
	term_end TEXT,
	announcement TEXT,
	link TEXT,
	floor FLOAT,
	description TEXT,
	image_count INT
)

select * from tenders where organizer = 'Стоян Благоев Якимов' order by published desc

select organizer, count(*), sum(price) from tenders group by 1 order by 3 desc;

select * from tenders;

select sum(1) from holmes

SELECT                                                                                                                  
      generate_series(min(measurement_day)::DATE, max(measurement_day)::DATE, '1 DAY'::INTERVAL)::date dat
FROM holmes
