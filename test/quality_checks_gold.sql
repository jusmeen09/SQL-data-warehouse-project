



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
