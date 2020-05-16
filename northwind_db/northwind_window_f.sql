 WITH orders_grouped as (
select orderid, orderdate, sum(od.unitprice * od.quantity - (od.unitprice * od.quantity * od.discount)) as order_price
	FROM orders 
LEFT JOIN order_details od using(orderid)
GROUP BY 1, 2
),
running as (
select 
	date(date_trunc('month', orderdate)) as month,
	extract(day from orderdate) as day,
	orderid,
	orderdate,
	order_price,
	SUM(order_price) OVER (PARTITION BY date_trunc('month', orderdate)
						   ORDER BY orderdate, orderid) as running_order_price, 
	SUM(order_price) OVER (PARTITION BY date_trunc('month', orderdate)) as monthly_total,
	ROW_NUMBER() OVER (PARTITION BY date_trunc('month', orderdate)
						   ORDER BY orderdate, orderid) as rnk_in_month
from orders_grouped
order by 3
)
SELECT 
	*, ROUND(running_order_price * 100 / monthly_total, 2) as perc_of_monthly_total_to_date
FROM running
						   
---------------
--- Weekly moving average
---------------

WITH orders_grouped as (
select 
	to_char(orderdate, 'IYYY-IW') as yw, 
	sum(od.unitprice * od.quantity - (od.unitprice * od.quantity * od.discount)) as order_price
FROM orders 
LEFT JOIN order_details od using(orderid)
GROUP BY 1
)
select 
	*,
	AVG(order_price) OVER (ORDER BY yw ROWS 2 PRECEDING) as ma_3,
	AVG(order_price) OVER (ORDER BY yw ROWS 5 PRECEDING) as ma_6 
from orders_grouped 
order by 1
		
-----------
--- What share of sales is coming from where
-----------
		
WITH sales_per_country as (
select 
	to_char(orderdate, 'YYYY-MM') as yw, 
	country,
	sum(od.unitprice * od.quantity - (od.unitprice * od.quantity * od.discount)) as order_price
from orders
left join order_details od using(orderid)
left join customers using(customerid)
group by 1, 2
)
select
	*,
	ROUND(order_price * 100. / SUM(order_price) OVER (PARTITION BY yw), 2) as share_of_total_monthly,
	SUM(order_price) OVER (PARTITION BY yw) as monthly_total
from sales_per_country
order by 1, 2
		
-------------
--- Total per country
-------------

WITH total_per_country as (
select 
	country,
	extract(year from orderdate) as year_,
	sum(od.unitprice * od.quantity - (od.unitprice * od.quantity * od.discount)) as order_price
from orders
left join order_details od using(orderid)
left join customers using(customerid)
group by 1, 2
)
SELECT 
	*,
	ROUND(order_price * 100. / sum(order_price) OVER (PARTITION BY year_), 2) as share_of_all
FROM total_per_country
ORDER BY 4 DESC