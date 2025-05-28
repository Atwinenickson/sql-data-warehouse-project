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
