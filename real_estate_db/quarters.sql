select 
	type, place, area, price, round(price/area) as price_sqm, details, labels, link
from real_estate_db.daily 
where measurement_day = (select max(measurement_day) from real_estate_db.daily)
and is_apartment 
and is_for_sale 
and place = 'изгрев'
and price < 120000
and round(price/area) < 1300
and type = 'тристаен'
order by 5


select 
	place,
	type,
	count(*) as row_count,
	cast(min(area) as varchar)||' - '||cast(max(area) as varchar) as area_range,
	round(avg(area)) as avg_area,
	approx_percentile(area, 0.5) as median_area,
	cast(min(price) as varchar)||' - '||cast(max(price) as varchar) as price_range, 
	round(avg(price)) as avg_price, 
	approx_percentile(price, 0.5) as median_price,
	round(avg(round(price/area))) as avg_price_sqm,
	approx_percentile(round(price/area), 0.5) as median_price_sqm
from real_estate_db.daily 
where measurement_day = (select max(measurement_day) from real_estate_db.daily)
and is_apartment 
and is_for_sale 
and regexp_like(place, 'изгрев|^младост|брези$|^дружба|иван вазов|^изток|слатина|лозенец|хаджи димитър$|оборище|хладилника$|^център|яворов')
and price > 0
and area > 0
and not regexp_like(place, '^с.')
group by 1, 2
order by 1, 11