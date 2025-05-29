/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        set @batch_start_time = GETDATE()
        PRINT '==================================================================================';
        PRINT 'LOADING SILVER LAYER';
        PRINT '==================================================================================';

        PRINT '--------------------------------------------------';
        PRINT 'LOADING CRM TABLES'
        PRINT '--------------------------------------------------';

        -- LOADING silver.crm_cust_info.
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: silver.crm_cust_info)'
        TRUNCATE TABLE silver.crm_cust_info
        PRINT '>> INSERTING DATA INTO: silver.crm_cust_info'
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_data)

        SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END cst_marital_status,
        CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END cst_gndr,
        cst_create_data
        FROM (
            SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_data DESC) as flag_last
            FROM  bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        )t WHERE flag_last = 1
        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' +CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS'
        PRINT '>> ----------------------';

        -- LOADING silver.crm_prd_info.
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: silver.crm_prd_info)'
        TRUNCATE TABLE silver.crm_prd_info
        PRINT '>> INSERTING DATA INTO: silver.crm_prd_info'
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
        prd_nm,
        ISNULL(prd_cost, 0) AS prd_cost,
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CASE
            WHEN prd_end_dt < prd_start_dt THEN CAST(prd_end_dt AS DATE)
            ELSE CAST(prd_start_dt AS DATE)
        END prd_start_dt,
        CASE
            WHEN LEAD(prd_end_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) IS NOT NULL AND prd_end_dt IS NOT NULL
                THEN CAST(LEAD(prd_end_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)- 1 AS DATE)
            WHEN LEAD(prd_end_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) IS NULL AND prd_end_dt IS NOT NULL
                THEN CAST(prd_start_dt AS DATE)
            ELSE NULL
        END prd_end_dt

        FROM bronze.crm_prd_info
        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' +CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS'
        PRINT '>> ----------------------';



        -- LOADING silver.crm_sales_details.
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: silver.crm_sales_details)'
        TRUNCATE TABLE silver.crm_sales_details
        PRINT '>> INSERTING DATA INTO: silver.crm_sales_details'
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )

        SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,
        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS varchar) AS date)
        END AS sls_ship_dt,
        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS varchar) AS date)
        END AS sls_due_dt,
        CASE WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_sales*abs(sls_price)
            THEN sls_quantity * abs(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE WHEN sls_price is null or sls_price <=0
            THEN sls_sales/nullif(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
        FROM bronze.crm_sales_details
        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' +CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS'
        PRINT '>> ----------------------';

        -- LOADING silver.crm_sales_details.
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: silver.erp_cust_az12)'
        TRUNCATE TABLE silver.erp_cust_az12
        PRINT '>> INSERTING DATA INTO: silver.erp_cust_az12'
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
            ELSE cid
        END AS cid,
        CASE WHEN  bdate > GETDATE() THEN NULL
            ELSE bdate
        END as bdate,
        CASE
            WHEN UPPER(REPLACE(REPLACE(REPLACE(TRIM(gen), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(REPLACE(REPLACE(REPLACE(TRIM(gen), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
        FROM bronze.erp_cust_az12
        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' +CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS'
        PRINT '>> ----------------------';

        -- LOADING silver.erp_loc_a101.
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: silver.erp_loc_a101)'
        TRUNCATE TABLE silver.erp_loc_a101
        PRINT '>> INSERTING DATA INTO: silver.erp_loc_a101'
        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
        REPLACE(cid, '-', '') cid,
        CASE WHEN REPLACE(TRIM(cntry),CHAR(13), '')  = 'DE' THEN 'Germany'
            WHEN REPLACE(TRIM(cntry), CHAR(13), '') IN ('US', 'USA') THEN 'United States'
            WHEN REPLACE(TRIM(cntry), CHAR(13), '') = '' OR cntry IS NULL THEN 'n/a'
            ELSE REPLACE(TRIM(cntry), CHAR(13), '')
        END AS cntry
        FROM bronze.erp_loc_a101
        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' +CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS'
        PRINT '>> ----------------------';


        -- LOADING silver.crm_sales_details.
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: silver.erp_px_cat_g1v2'
        TRUNCATE TABLE silver.erp_px_cat_g1v2
        PRINT '>> INSERTING DATA INTO: silver.erp_px_cat_g1v2'
        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT
        id,
        cat,
        subcat,
        REPLACE(TRIM(maintenance), CHAR(13), '')
        FROM bronze.erp_px_cat_g1v2
        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION: ' +CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS'
        PRINT '>> ----------------------';

        SET @batch_end_time = GETDATE()
        PRINT '===================================================================';
        PRINT 'LOADING SILVER LAYER IS COMPLETED';
        PRINT ' - TOTAL LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'SECONDS'
        PRINT '======================================================================';
    END TRY
    BEGIN CATCH
        PRINT '=======================================================================';
        PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
        PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '======================================================================';
    END CATCH
END
