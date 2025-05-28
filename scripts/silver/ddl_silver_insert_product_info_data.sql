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
