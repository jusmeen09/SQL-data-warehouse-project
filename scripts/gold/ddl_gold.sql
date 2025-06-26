/*
==================================================================
DDL script:  Create Gold views
==================================================================
Script purpose:
    This script creates views for the gold layer in the data warehouse.
    The gold layer represents the final dimension and fact tables(Star Schema)

    Each view performs transformations and combine data from the silver layer
    to produce a clean,enriched and business-ready dataset

Usage:
  - These views can be queried directly for analytics and reporting 
====================================================================
*/
--================================================================
--CREATE DIMENSION:GOLD.DIM_CUSTOMERS
--=================================================================
create view gold.dim_customers as 
SELECT 
row_number () over (order by cst_id)as customer_key,
ci.cst_id as customer_Id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
case when ci.cst_gndr!= 'N/A' then ci.cst_gndr
else ca.gen 
end as gender,
ca.bdate as birth_date,
ci.cst_create_date as create_date
FROM SILVER.CRM_CUST_INFO as ci
left join silver.erp_cust_az12 as ca
on ci.cst_key=ca.cid
left join silver.erp_loc_A101 as la
on ci.cst_key=la.cid


--================================================================
--CREATE DIMENSION:GOLD.DIM_PRODUCTS
--=================================================================

create view gold.dim_product as 
select 
row_number() over (order by pn.prd_start_dt,pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as catgeory_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance as maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info as pn
left join silver.erp_px_cat_G1V2 as pc
on pn.cat_id=pc.id
where pn.prd_end_dt is null--- filter out histotical data

--================================================================
--CREATE DIMENSION:GOLD.FACT_SALES
--=================================================================
create view gold.fact_sales as 
select 
sd.sls_ord_num as order_number,
pr.product_key,
c.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as ship_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details as sd
left join gold.dim_product as pr
on sd.sls_prd_key=pr.product_number
left join gold.dim_customers as c
on sd.sls_cust_id= c.customer_Id





