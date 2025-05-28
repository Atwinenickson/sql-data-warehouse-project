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
