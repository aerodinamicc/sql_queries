create database real_estate owner postgres;

create role aerodinamicc;

select max(measurement_day) from daily_measurements dm;

select 
	measurement_day, 
	sum(case when site = 'imoteka.bg' then 1 else 0 end) as imoteka,
	sum(case when site = 'address.bg' then 1 else 0 end) as address,
	sum(case when site = 'arcoreal.bg' then 1 else 0 end) as arcoreal,
	sum(case when site = 'yavlena.com' then 1 else 0 end) as yavlena,
	sum(case when site = 'sofia.holmes.bg' then 1 else 0 end) as holmes,
	sum(case when site = 'www.superimoti.bg' then 1 else 0 end) as superimoti
from daily_measurements dm2 
group by 1
order by 1 desc;

select measurement_day, count(*) from holmes group by 1 order by 1 desc;


select distinct site from daily_measurements dm;

with periods as (
select min(measurement_day) as m_day from holmes 
union 
select max(measurement_day) as m_day from holmes
), agg as (
select 
	trim(substring(place from '^[^\d]+')) as place,
	round(avg(case when title = 'едностаен апартамент' and measurement_day = (select min(m_day) from periods) then price_sqm else null end)) as one_room_before,
	round(avg(case when title = 'едностаен апартамент' and measurement_day = (select max(m_day) from periods) then price_sqm else null end)) as one_room_after,
	round(avg(case when title = 'двустаен апартамент' and measurement_day = (select min(m_day) from periods) then price_sqm else null end)) as two_rooms_before,
	round(avg(case when title = 'двустаен апартамент' and measurement_day = (select max(m_day) from periods) then price_sqm else null end)) as two_rooms_after,
	round(avg(case when title = 'тристаен апартамент' and measurement_day = (select min(m_day) from periods) then price_sqm else null end)) as three_rooms_before,
	round(avg(case when title = 'тристаен апартамент' and measurement_day = (select max(m_day) from periods) then price_sqm else null end)) as three_rooms_after,
	round(avg(case when title = 'четиристаен апартамент' and measurement_day = (select min(m_day) from periods) then price_sqm else null end)) as four_rooms_before,
	round(avg(case when title = 'четиристаен апартамент' and measurement_day = (select max(m_day) from periods) then price_sqm else null end)) as four_rooms_after,
	round(avg(case when title = 'многостаен апартамент' and measurement_day = (select min(m_day) from periods) then price_sqm else null end)) as five_rooms_before,
	round(avg(case when title = 'многостаен апартамент' and measurement_day = (select max(m_day) from periods) then price_sqm else null end)) as five_rooms_after,
	round(avg(case when measurement_day = (select min(m_day) from periods) then price_sqm else null end)) as overall_before,
	round(avg(case when measurement_day = (select max(m_day) from periods) then price_sqm else null end)) as overall_after
from holmes h 
where measurement_day IN (select distinct m_day from periods)
and title ~ 'апартамент'
and not (place ~ '^с.|^в.з|^м-т|ПЗ|^ж.гр.|^гр.')
and substring(place from '^[^\d]+') is not null
group by 1
having count(*) > 50
order by 1)
select 
	place, 
	one_room_before, one_room_after, 100 - round((one_room_before * 100.)/one_room_after, 2) as perc_change_1,
	two_rooms_before, two_rooms_after,  100 - round((two_rooms_before * 100.)/two_rooms_after, 2) as perc_change_2,
	three_rooms_before, three_rooms_after,  100 - round((three_rooms_before * 100.)/three_rooms_after, 2) as perc_change_3,
	four_rooms_before, four_rooms_after,  100 - round((four_rooms_before * 100.)/four_rooms_after, 2) as perc_change_4,
	five_rooms_before, five_rooms_after, 100 - round((five_rooms_before * 100.)/five_rooms_after, 2) as perc_change_5,
	overall_before, overall_after, 100 - round((overall_before * 100.)/overall_after, 2) as overall_perc_change
from agg
	


select distinct title
from holmes h 
WHERE not (place ~ '^с.|^в.з|^м-т|ПЗ|^ж.гр.|^гр.')
and title ~ 'апартамент'
and substring(place from '^[^\d]+') is not null;