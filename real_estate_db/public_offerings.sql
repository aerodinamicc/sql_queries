select 
	organizer, 
	round(sum(price), 2) as total_price, 
	round(avg(price), 2)  as avg_price, 
	count(*) as offers 
from tenders 
group by 1
order by 3 desc;

select * from tenders where lower(title) ~ 'апартамент|ателие|мезонет' and now() < to_date(term_end, 'DD.MM.YYYY') order by published desc ; 

select * from tenders where address = 'бул. Мадрид № 13, ет.2' --good
-- 'ул. Институтска № 1, ет. 3, ап. 7' good
--'град София, район Витоша, улица „Майстор Павел от Кримин“ № 15 ' good
--'гр. София, Столична община – район „Искър”, на ул. „Неделчо Бончев” №24'
--'гр. София, р-н Лозенец, ул. Горски пътник №42, ет.1, офис 1'

-- Properties offered more than once on a different price
select address, count(*) from tenders group by 1 having max(term_start) <> min(term_start) order by 2 desc

select title, count(*) from tenders group by 1 order by 2 desc

select 
	city,
	address, 
	count(distinct description) as dist_desc, 
	count(*) as rows
from tenders
group by 1, 2
having count(*) > 1
order by 4 desc;
