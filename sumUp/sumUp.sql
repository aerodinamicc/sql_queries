create table pentaho (
vendor_name varchar,
payment_term varchar,
country_code varchar,
annual_spend numeric,
term_length numeric,
discount_cliff numeric,
discount_percent numeric,
country varchar,
region varchar,
official_language varchar,
national_currency varchar,
gdp_for_capita_usd varchar,
max_tenor numeric)

drop table pentaho

select * from pentaho limit 10


-----
-- сумуп
-----

create table sumup (
	client_id numeric,
	transaction_date varchar,
	card_acceptor_country varchar,
	cardholder_country varchar,
	amount_eur numeric,
	source_currency varchar,
	response_code numeric,
	response_description varchar,
	enabled_at varchar,
	pos_entry_mode varchar,
	transaction_type varchar,
	tx_row_number numeric
)

copy sumup 
FROM 'D:/card_usage.csv' DELIMITER ',' CSV HEADER

DROP TABLE IF EXISTS card_transactions

CREATE TABLE card_transactions 
AS
SELECT 
	client_id, 
	TO_TIMESTAMP(transaction_date,'MM-DD-YYYY HH24:MI')  as transaction_date,
	card_acceptor_country, 
	cardholder_country, 
	amount_eur, 
	source_currency, 
	response_code, 
	response_description, 
	TO_TIMESTAMP(enabled_at,'MM-DD-YYYY HH24:MI') as enabled_at,
	pos_entry_mode, 
	transaction_type, 
	tx_row_number
FROM sumup

select * from card_transactions limit 10

select * from sumup limit 10

SELECT 
	client_id,
	transaction_date,
	amount_eur,
	CASE WHEN amount_eur > LAG(amount_eur) OVER (PARTITION BY client_id
												ORDER BY tx_row_number) --or transaction_date when parsed as timestamp
		 THEN 'Increased'
		 -- in addition to incrase/decrease it could be the same price so we excplicitly check for decrease
		 WHEN amount_eur < LAG(amount_eur) OVER (PARTITION BY client_id
												ORDER BY tx_row_number) 
		 THEN 'Decreased'
		 ELSE NULL END AS comparison
FROM card_transactions

	
