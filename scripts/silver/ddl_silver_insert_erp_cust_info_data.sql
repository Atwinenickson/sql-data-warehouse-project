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
