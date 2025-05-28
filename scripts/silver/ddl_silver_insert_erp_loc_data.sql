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
