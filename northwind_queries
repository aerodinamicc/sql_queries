----- B2B

  select a.companyname, b.companyname, a.country
	  from suppliers a
	  inner join (select companyname, country from suppliers) b
		  ON a.country = b.country
	  where a.companyname <> b.companyname
	  order by a.country

---- least sold products
select products.productname, sum(od.unitprice*od.quantity)
	from order_details od
	left join products using (productid)
	group by 1
	having sum(od.unitprice*od.quantity) < 2000
					
					  
SELECT categoryname,productname,SUM(od.unitprice*quantity)
FROM categories
NATURAL JOIN products
NATURAL JOIN order_details AS od
GROUP BY GROUPING SETS  ((categoryname),(categoryname,productname))
ORDER BY categoryname, productname;
										 
---- grouping sets
select c.companyname, s.companyname, round(sum(od.unitprice * od.quantity)::numeric, 2) as total_sum
	 from customers c
	 left join orders using(customerid)
	 left join order_details od using(orderid)
	 left join products using(productid)
	 left join suppliers s using(supplierid)
group by grouping sets ((c.companyname), (c.companyname, s.companyname))
order by c.companyname, s.companyname NULLS FIRST
										  
----- ROLLUP
										  
select c.companyname, cat.categoryname, products.productname, round(sum(od.unitprice * od.quantity)::numeric, 2) as total_sum
	 from customers c
	 left join orders using(customerid)
	 left join order_details od using(orderid)
	 left join products using(productid)
	 left join categories cat using(categoryid)
group by ROLLUP ( companyname, categoryname, productname)
order by 1, 2, 3
										  
select s.companyname, products.productname, customers.companyname, round(sum(od.unitprice * od.quantity)::numeric, 2) as total_sum
	 from suppliers s
	 left join products using(supplierid)
	 left join order_details od using(productid)
	 left join orders using(orderid)
	 left join customers using(customerid)
group by ROLLUP ( s.companyname, products.productname, customers.companyname)
order by 1, 2, 3

---- CUBE
select companyname, categoryname, productname, round(sum(od.unitprice * od.quantity)::numeric, 2) as total_sum
	 from customers c
	 left join orders using(customerid)
	 left join order_details od using(orderid)
	 left join products using(productid)
	 left join categories cat using(categoryid)
group by CUBE ( companyname, categoryname, productname)
order by 1, 2, 3
										  
										  
------ UNION ALL
select country from customers
	  union
select country from suppliers
order by 1
										  
------ INTERSECT
select country from customers
	  intersect
select country from suppliers
order by 1
										  
select count(*) from (
	select city from customers
	intersect all
	select city from suppliers) as same_city
										  
---EXCEPT
										  
select count(*) from (
	select city from suppliers 
	except
	select city from customers) as same_city
										  
----- All suppliers with a product that costs more than 200USD
select companyname
from suppliers
where exists (select productid from products
		      where products.supplierid = suppliers.supplierid
			    and unitprice > 200)
										  
--- All suppliers that don't have an  order in Dec1996
select companyname
	from suppliers
	where not exists (select productid from products
				   join order_details as od using(productid)
				   join orders as o using(orderid)
					where products.supplierid = suppliers.supplierid
					  and o.orderdate between '1996-12-01' AND '1996-12-31')
										  
---- ANY and ALL
------- Find all suppliers that have an order with 1 item
										  
select companyname
  from suppliers
  where supplierid = ANY (select supplierid from products
						 join order_details using(productid)
						 where quantity = 1)
										  
------------ All dist customers that ordered more in one item than the average order amount per item of all customers

select distinct companyname from customers
	  join orders using(customerid)
	  join order_details od using(orderid)
	  where od.unitprice * od.quantity > ALL (select avg(od.unitprice * od.quantity)
												 from order_details as od
												 join orders using(orderid)
												 group by customerid)



					  
					 