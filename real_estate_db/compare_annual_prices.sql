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

select count(*) from daily_measurements dm 

--dedup daily_measurements 
select count(distinct(id)) from (
select id from daily_measurements dm 
where measurement_day = '2020-10-13'
and site = 'imoteka.bg'
group by id 
having count(*) > 1
) v

drop table if exists daily_measurements;
alter table daily_measurements_dedup rename to daily_measurements;

select site, id, is_for_sale, price, labels, views, measurement_day 
into daily_measurements_dedup
from daily_measurements dm 
group by 1, 2, 3, 4, 5, 6, 7

with external_data as (
--https://money.bg/property/kakvi-sa-aktualnite-tseni-na-zhilishtata-v-otdelnite-kvartali-na-sofiya.html?mc_cid=0b0ee9bc68&mc_eid=3587fcff4e
SELECT 'Борово' as place, 1228 as q1_2020,	1250 as q1_2021
UNION
SELECT 'Витоша' as place, 1049 as q1_2020,	1110 as q1_2021
UNION
SELECT 'Гео Милев' as place, 1332 as q1_2020,	1386 as q1_2021
UNION
SELECT 'Дружба' as place, 903 as q1_2020,	930 as q1_2021
UNION
SELECT 'Иван Вазов' as place, 1738 as q1_2020,	1860 as q1_2021
UNION
SELECT 'Изток' as place, 1702 as q1_2020,	1808 as q1_2021
UNION
SELECT 'Красно село' as place, 1261 as q1_2020,	1346 as q1_2021
UNION
SELECT 'Кръстова вада' as place, 1263 as q1_2020,	1299 as q1_2021
UNION
SELECT 'Лозенец' as place,	1626  as q1_2020, 1773 as q1_2021
UNION
SELECT 'Люлин' as place, 914  as q1_2020, 976 as q1_2021
UNION
SELECT 'Манастирски ливади' as place, 1103  as q1_2020, 1167 as q1_2021
UNION
SELECT 'Младост' as place, 1281  as q1_2020, 1332 as q1_2021
UNION
SELECT 'Надежда' as place, 911  as q1_2020, 958 as q1_2021
UNION
SELECT 'Овча купел' as place, 959  as q1_2020, 1002	 as q1_2021
UNION
SELECT 'Хаджи Димитър' as place, 990  as q1_2020, 1021 as q1_2021
UNION
SELECT 'Център' as place, 1520  as q1_2020, 1640 as q1_2021
UNION
SELECT 'Яворов' as place, 1619  as q1_2020, 1684 as q1_2021
UNION
SELECT 'Coфия' as place, 1259  as q1_2020, 1326 as q1_2021
),
periods as (
select min(measurement_day) as m_day from holmes 
union 
select max(measurement_day) as m_day from holmes
), 
agg as (
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
order by 1),
stats as (
select 
	agg.place,
	q1_2020, q1_2021, 100 - round((q1_2020 * 100.)/q1_2021, 2) as imoteka_perc_change,
	overall_before, overall_after, 100 - round((overall_before * 100.)/overall_after, 2) as overall_perc_change,
	one_room_before, one_room_after, 100 - round((one_room_before * 100.)/one_room_after, 2) as perc_change_1,
	two_rooms_before, two_rooms_after,  100 - round((two_rooms_before * 100.)/two_rooms_after, 2) as perc_change_2,
	three_rooms_before, three_rooms_after,  100 - round((three_rooms_before * 100.)/three_rooms_after, 2) as perc_change_3,
	four_rooms_before, four_rooms_after,  100 - round((four_rooms_before * 100.)/four_rooms_after, 2) as perc_change_4,
	five_rooms_before, five_rooms_after, 100 - round((five_rooms_before * 100.)/five_rooms_after, 2) as perc_change_5
from agg
left join external_data on agg.place = trim(external_data.place)
)
select * from stats