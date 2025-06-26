/*
=============================================================
Quality checks
=============================================================
Script purpose:
     This script performs various quality checks for data consistency,accuracy,and standarization
     across the 'silver' schemas. It include checks for:
    -NULL or duplicate primary keys.
    -Unwanted spaces in string fields.
    -DATA STANDARIZATION AND CONSISTENCY
    -Invalid data ranges and orders
    - Data consistency between related fileds

Usage notes:
    -Run these checks after data loading into silver layer.
    -Investigate and resolve any discrepancies found during the checks.
=======================================================================
*/

--============================================================
Checking silver.crm_cust_info
--=============================================================

select * from silver.crm_cust_info;
--CHECK FOR DUPLICATES AND NULL IN PRIMARY KEY
--EXPECTATION NULL
select cst_id,count(*) as flag_last
from silver.crm_cust_info group by cst_id having count(*) >1;


--- REMOVE UNWANTED STATUS

SELECT cst_firstname FROM silver.crm_cust_info where cst_firstname!=trim(cst_firstname);
SELECT cst_lastname FROM silver.crm_cust_info where cst_lastname!=trim(cst_lastname);


---DATA STANDARIZATION

SELECT DISTINCT(CST_MARITAL_STATUS) from silver.crm_cust_info;
SELECT DISTINCT(CST_gndr) from silver.crm_cust_info;

--============================================================
Checking silver.crm_sales_details
--=============================================================

-----check invalid date
select
nullif(sls_due_dt,0) as sls_due_dt
from silver.crm_sales_details
where sls_due_dt<=0
or length (sls_due_dt::text)!=8
----check for invalid dates orders

select * from silver.crm_sales_details
where sls_order_dt>sls_ship_dt or sls_order_dt>sls_due_dt;

--check data consistency:between sales,quantity, and price 
--sales=quantity*price


select distinct 
 sls_sales ,
 sls_quantity ,
 sls_price 
	  from  silver.crm_sales_details
where  sls_sales!=sls_price*sls_quantity 
or sls_sales is null or sls_price is null or sls_quantity is null
or sls_sales <=0 or sls_price  <=0 or sls_quantity <=0

select * FROM silver.crm_sales_details

--============================================================
Checking silver.crm_prd_info
--=============================================================
  
select * from silver.crm_prd_info;
select prd_id,count(*) from silver.crm_prd_info 
group by prd_id having count(*)>1 or  prd_id is  null;

---check for unwanted space
--exp:no result
SELECT PRD_NM FROM silver.CRM_PRD_INFO WHERE PRD_NM!=TRIM(PRD_NM)

--check for null and negative numbers
-- exp:no result
SELECT PRD_COST FROM silver.CRM_PRD_INFO
WHERE PRD_COST IS NULL OR PRD_COST <0

--data standarization & consistency
select distinct(prd_line) from silver.crm_prd_info

---check for invalid date orders
select * from silver.crm_prd_info
where prd_end_dt<prd_start_dt

  --============================================================
Checking SILVER.ERP_CUST_AZ12
--=============================================================
--IDENTIFY OUT OF RANGE DATES---
SELECT DISTINCT(BDATE) FROM SILVER.ERP_CUST_AZ12
where BDATE< '1900-01-01'OR BDATE>NOW() ;
-- DATA STANDARIZATION AND CONSISTENCY
SELECT DISTINCT(GEN) FROM SILVER.ERP_CUST_AZ12;

SELECT * FROM SILVER.ERP_CUST_AZ12;
--============================================================
Checking silver.ERP_LOC_A101
--=============================================================
--data standarization and consitency 
SELECT DISTINCT (CNTRY) FROM silver.ERP_LOC_A101 order by cntry;


