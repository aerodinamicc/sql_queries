select 
	neighborhood,
	count(distinct(id))  as imots , 
   avg(price_sqm) as avg_price_sqm, 
   avg(price) as avg_price,
	percentile_disc(0.5) within group (order by price)
from imoti
where measurement_day = (select max(measurement_day) from imoti)
and title = 'ПАРЦЕЛ'
and (neighborhood like '%Железница' or neighborhood like '%Бистрица' or neighborhood like '%Панчарево')
group by 1
order by 2 desc

select * 
from (
select 
	trim(details->>'Регулация:') as regulation,
	trim(details->>'Вода:') as water,
	trim(details->>'Ток:') as electricity,
	*
from imoti
where measurement_day = (select max(measurement_day) from imoti)
and title = 'ПАРЦЕЛ'
and neighborhood like '%Желява'--'с%'
--and lon < 42.6266 --Ring road
and area < 1001
and price < 40000
and details->>'Регулация:' is not null
and details->>'Вода:' is not null
and details->>'Ток:' is not null
) а
where regulation like 'ДА' and water like 'ДА' and electricity like 'ДА'
order by price