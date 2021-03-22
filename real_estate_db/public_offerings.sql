select 
	organizer, 
	round(sum(price), 2) as total_price, 
	round(avg(price), 2)  as avg_price, 
	count(*) as offers 
from tenders 
group by 1
order by 3 desc