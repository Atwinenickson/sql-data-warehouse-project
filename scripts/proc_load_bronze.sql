CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        set @batch_start_time = GETDATE();
        PRINT '==================================================================================';
        PRINT 'LOADING BRONZE LAYER'
        PRINT '==================================================================================';

        PRINT '----------------------------------------------------------------------------------';
        PRINT 'LOADING CRM TABLES'
        PRINT '----------------------------------------------------------------------------------';

        SET @start_time = GETDATE()
        PRINT '>>TRUNCATING TABLE bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> INSERTING DATA INTO: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/datasets/source_crm/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        PRINT '>> DONE INSERTING DATA INTO: bronze.crm_cust_info';
        SET @end_time = GETDATE()
        PRINT '>> LOAD DURATION WAS: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
        PRINT '>>------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>>TRUNCATING TABLE bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> INSERTING DATA INTO: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/datasets/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        PRINT '>>DONE INSERTING DATA INTO: bronze.crm_prd_info';
        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION WAS: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
        PRINT '>>------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>>TRUNCATING TABLE bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> INSERTING DATA INTO: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/datasets/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        PRINT '>>DONE INSERTING DATA INTO: bronze.crm_sales_details';
        SET @end_time = GETDATE()
        PRINT '>> LOAD DURATION WAS: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
        PRINT '>>------------------------------------';


        PRINT '----------------------------------------------------------------------------------';
        PRINT 'LOADING ERP TABLES'
        PRINT '----------------------------------------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>>TRUNCATING TABLE bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> INSERTING DATA INTO: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/datasets/source_erp/loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        PRINT '>>DONE INSERTING DATA INTO: bronze.erp_loc_a101';
        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION WAS: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
        PRINT '>>------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>>TRUNCATING TABLE bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> INSERTING DATA INTO: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/datasets/source_erp/cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        PRINT '>>DONE INSERTING DATA INTO: bronze.erp_cust_az12';
        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION WAS: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
        PRINT '>>------------------------------------';

        SET @start_time = GETDATE()
        PRINT '>>TRUNCATING TABLE bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> INSERTING DATA INTO: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/datasets/source_erp/px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        PRINT '>>DONE INSERTING DATA INTO: bronze.erp_px_cat_g1v2';
        SET @end_time = GETDATE();
        PRINT '>> LOAD DURATION WAS: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
        PRINT '>>------------------------------------';
        SET @batch_end_time = GETDATE();
        PRINT '==============================================================';
        PRINT ' LOADING BRONZE LAYER IS COMPLETED..............';
        PRINT(' - TOTAL LOAD DURATION: ' +CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' SECONDS');
        PRINT '===============================================================';
    END TRY
    BEGIN CATCH
        PRINT '=======================================================================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '======================================================================';
    END CATCH
END



-- RUN SQL CODE TO LOAD THE STORED PROCEDURE
EXEC bronze.load_bronze
