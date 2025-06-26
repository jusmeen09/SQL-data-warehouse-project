/*
=============================================================
Quality checks
=============================================================
Script purpose:
     This script performs various quality checks for data consistency, accuracy of the gold layer .
      It include checks for:
    -Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage notes:
    -Run these checks after data loading into gold layer.
    -Investigate and resolve any discrepancies found during the checks.
=======================================================================
*/



--===========================================================
  --Checking 'gold.dim_customer'
--===========================================================
--check for uniqueness of product key in gold.dim_products
--expectation:no result
  select
       customer_key,
       count(*) as duplicate
  from gold.dim_customers
  group by customer_key
  having count(*)>1;
  
--===========================================================
  --Checking 'gold.dim_product
--===========================================================
--check for uniqueness of product key in gold.dim_products
--expectation:no result
  select
       product_key,
       count(*) as duplicate
  from gold.dim_product
  group by product_key
  having count(*)>1;
--===========================================================
  --Checking 'gold.fact_sales'
--===========================================================
--check the data model connectivity between fact and dimensions
select * from gold.dim_customers c
right join gold.fact_sales as s
on s.customer_key=c.customer_key
right join gold.dim_product as p 
on s.product_key=p.product_key
where  p.product_key is null
