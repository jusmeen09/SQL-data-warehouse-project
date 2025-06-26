/*
====================================================
Stored Procedure:Load silver layer (bronze--> silver)
=====================================================
Script Purpose:
    This stored procedure performs the ETL(EXtract,Tranform,Load) process to
populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
   -Truncate silver tables
   -Insert transformed and cleaned data from bronze into silver tables.

Paramters:
   None
  This stored procedure does not accept any paramters or return any values.

Usage Example:
     call silver.load_silver();
=========================================================================
*/
CREATE OR REPLACE PROCEDURE silver.load_silver()
AS $$
declare
	v_start_time timestamp;
	v_end_time timestamp;
	v_duration interval;
BEGIN
	v_start_time:=clock_timestamp();
	
     RAISE NOTICE 'truncating table silver.crm_cust_info';
     TRUNCATE TABLE silver.crm_cust_info;

     RAISE NOTICE 'INSERTING DATA INTO:silver.crm_cust_info';
     INSERT INTO silver.crm_cust_info(
         cst_id,
         cst_key,
         cst_firstname,
         cst_lastname,
         cst_marital_status,
         cst_gndr,
         cst_create_date)
     SELECT
         CST_ID,
         CST_KEY,
         TRIM(CST_FIRSTNAME),
         TRIM(CST_LASTNAME),
         CASE 
             WHEN UPPER(TRIM(CST_MARITAL_STATUS)) = 'M' THEN 'Married'
             WHEN UPPER(TRIM(CST_MARITAL_STATUS)) = 'S' THEN 'Single'
             ELSE 'N/A'
         END,
         CASE 
             WHEN UPPER(TRIM(CST_GNDR)) = 'M' THEN 'Male'
             WHEN UPPER(TRIM(CST_GNDR)) = 'F' THEN 'Female'
             ELSE 'N/A'
         END,
         CST_CREATE_DATE
     FROM (
         SELECT *,
                ROW_NUMBER() OVER (PARTITION BY CST_ID ORDER BY CST_CREATE_DATE DESC) AS flag_last
         FROM bronze.crm_cust_info
         WHERE cst_id IS NOT NULL
     ) t
     WHERE flag_last = 1;

     RAISE NOTICE 'truncating table silver.crm_prd_info';
     TRUNCATE TABLE silver.crm_prd_info;

     RAISE NOTICE 'INSERTING DATA INTO:silver.crm_prd_info';
     INSERT INTO silver.crm_prd_info (
         prd_id,
         cat_id,
         prd_key,
         prd_nm,
         prd_cost,
         prd_line,
         prd_start_dt,
         prd_end_dt)
     SELECT 
         prd_id,
         REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
         SUBSTRING(prd_key, 7, LENGTH(prd_key)),
         prd_nm,
         COALESCE(prd_cost, 0),
         CASE 
             WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
             WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
             WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
             WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
             ELSE 'n/a'
         END,
         CAST(prd_start_dt AS date),
         CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' AS date)
     FROM bronze.crm_prd_info;

     RAISE NOTICE 'truncating table silver.crm_sales_details';
     TRUNCATE TABLE silver.crm_sales_details;

     RAISE NOTICE 'INSERTING DATA INTO:silver.crm_sales_details';
     INSERT INTO silver.crm_sales_details(
         sls_ord_num,
         sls_prd_key,
         sls_cust_id,
         sls_order_dt,
         sls_ship_dt,
         sls_due_dt,
         sls_sales,
         sls_quantity,
         sls_price)
     SELECT 
         sls_ord_num,
         sls_prd_key,
         sls_cust_id,
         CASE
             WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) != 8 THEN NULL
             ELSE CAST(CAST(sls_order_dt AS varchar) AS date)
         END,
         CASE
             WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
             ELSE CAST(CAST(sls_ship_dt AS varchar) AS date)
         END,
         CASE
             WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) != 8 THEN NULL
             ELSE CAST(CAST(sls_due_dt AS varchar) AS date)
         END,
         CASE
             WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_price) * sls_quantity
             THEN sls_quantity * ABS(sls_price)
             ELSE sls_sales
         END,
         sls_quantity,
         CASE
             WHEN sls_price IS NULL OR sls_price <= 0
             THEN sls_sales / NULLIF(sls_quantity, 0)
             ELSE sls_price
         END
     FROM bronze.crm_sales_details;

     RAISE NOTICE 'truncating table silver.erp_cust_AZ12';
     TRUNCATE TABLE silver.erp_cust_AZ12;

     RAISE NOTICE 'INSERTING DATA INTO:silver.erp_cust_AZ12';
     INSERT INTO silver.erp_cust_AZ12(
         CID,
         BDATE,
         GEN)
     SELECT 
         CASE 
             WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LENGTH(CID))
             ELSE CID
         END,
         CASE 
             WHEN BDATE < '1900-01-01' OR BDATE > NOW() THEN NULL
             ELSE BDATE
         END,
         CASE 
             WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
             WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
             ELSE 'n/a'
         END
     FROM bronze.erp_cust_AZ12;

     RAISE NOTICE 'truncating table silver.erp_loc_A101';
     TRUNCATE TABLE silver.erp_loc_A101;

     RAISE NOTICE 'INSERTING DATA INTO:silver.erp_loc_A101';
     INSERT INTO silver.erp_loc_A101(
         cid,
         cntry)
     SELECT 
         REPLACE(CID, '-', ''),
         CASE 
             WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
             WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
             WHEN TRIM(CNTRY) = '' OR TRIM(CNTRY) IS NULL THEN 'n/a'
             ELSE TRIM(CNTRY)
         END
     FROM bronze.erp_loc_A101;

     RAISE NOTICE 'truncating table silver.erp_px_cat_g1v2';
     TRUNCATE TABLE silver.erp_px_cat_g1v2;

     RAISE NOTICE 'INSERTING DATA INTO:silver.erp_px_cat_g1v2';
     INSERT INTO silver.erp_px_cat_g1v2(
         id,
         cat,
         subcat,
         maintenance)
     SELECT 
         id,
         cat,
         subcat,
         maintenance
     FROM bronze.erp_px_cat_g1v2;
	 
	V_END_TIME:=clock_timestamp();
	v_duration:=V_end_TIME - V_start_TIME;

	RAISE NOTICE'Procedure silver.load_silver is completed';
	RAISE NOTICE' Total Duration taken:%',v_duration;
	
  EXCEPTION
     WHEN OTHERS THEN
       RAISE NOTICE 'Something went wrong: %', SQLERRM;
  
END;
$$ LANGUAGE plpgsql;
