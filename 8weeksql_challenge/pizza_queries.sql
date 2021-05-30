/*
Pizza Metrics
How many pizzas were ordered?
How many unique customer orders were made?
How many successful orders were delivered by each runner?
How many of each type of pizza was delivered?
How many Vegetarian and Meatlovers were ordered by each customer?
What was the maximum number of pizzas delivered in a single order?
For each customer, how many delivered pizzas had at least 1 change and how many had no changes?
How many pizzas were delivered that had both exclusions and extras?
What was the total volume of pizzas ordered for each hour of the day?
What was the volume of orders for each day of the week?
 */

SELECT
	runners.runner_id,
    runners.registration_date,
	COUNT(DISTINCT runner_orders.order_id) AS orders
FROM runners
INNER JOIN runner_orders
	ON runners.runner_id = runner_orders.runner_id
WHERE runner_orders.cancellation IS NOT NULL
GROUP BY
	runners.runner_id,
    runners.registration_date;

 --1, 2
select 
	count(*) as pizza_count,
	count(distinct(order_id)) as unique_orders
from customer_orders;

--3
select runner_id, count(distinct(order_id)) from customer_orders co 
join runner_orders ro using(order_id)
where not lower(ro.cancellation) ~ '(?i)cancellation' or ro.cancellation is null
group by 1;

--4, 5
select pizza_name, co.customer_id, count(*) from customer_orders co 
join pizza_names pn using(pizza_id)
join runner_orders ro using(order_id)
where not ro.cancellation ~ '(?i)cancellation' or ro.cancellation is null
group by grouping sets ( (1), (1,2) );

--6
select order_id, count(*)
from customer_orders co 
join runner_orders ro using(order_id)
where not lower(ro.cancellation) ~ '(?i)cancellation' or ro.cancellation is null
group by 1
order by 2 desc limit 1;

--7
select 
	customer_id, 
	sum(case when exclusions in ('', 'null') or exclusions is null then 0 else 1 end) as eclusions, 
	sum(case when extras in ('', 'null') or extras is null then 0 else 1 end) as extras
from customer_orders co 
join runner_orders ro using(order_id)
where not lower(ro.cancellation) ~ '(?i)cancellation' or ro.cancellation is null
group by 1
order by 1;

--8
select 
	count(*)
from customer_orders co 
join runner_orders ro using(order_id)
where (not lower(ro.cancellation) ~ '(?i)cancellation' or ro.cancellation is null)
and not (exclusions in ('', 'null') or exclusions is null) 
and not (extras in ('', 'null') or extras is null);

--9
select 
	date_trunc('hour', order_time::timestamp),
	count(*)
from customer_orders co 
join runner_orders ro using(order_id)
where (not lower(ro.cancellation) ~ '(?i)cancellation' or ro.cancellation is null)
group by 1
order by 1;

--10
select 
	extract(isodow from order_time::timestamp)as dow,
	count(*)
from customer_orders co 
join runner_orders ro using(order_id)
where (not lower(ro.cancellation) ~ '(?i)cancellation' or ro.cancellation is null)
group by 1
order by 1;

/*
Runner and Customer Experience
How many runners signed up for each 1 week period? (i.e. week starts 2021-01-01)
What was the average time in minutes it took for each runner to arrive at the Pizza Runner HQ to pickup the order?
Is there any relationship between the number of pizzas and how long the order takes to prepare?
What was the average distance travelled for each customer?
What was the difference between the longest and shortest delivery times for all orders?
What was the average speed for each runner for each delivery and do you notice any trend for these values?
What is the successful delivery percentage for each runner?
*/

	