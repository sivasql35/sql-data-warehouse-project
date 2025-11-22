/*
***********************************************************************
		Stored Procedure: Load Bronze Layer (Source -> Bronze)
***********************************************************************
Script Purpose: 
	This stored procedure loads the data into the 'bronze' schema from 
	external csv files.
	It performs the following actipns: 
	- Truncates the bronze tables before loading the data,
	- Uses the 'BULK INSERT' command to load the data from CSV files to
	  bronze tables
Parameters:
	None,
	This stored procedure does not accept any parameters or return any 
	values.

Usage example:
	EXEC bronze.load_bronze;
***********************************************************************
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
	SET @batch_start_time = GETDATE();
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY	
		PRINT '---------------------==============================-------------------------';
		PRINT '						   Loading Bronze Layer';
		PRINT '---------------------==============================-------------------------';

		PRINT '------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting data into: crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Sivananda\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milli seconds';
		PRINT '------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: crm_prd_info';
		TRUNCATE TABLE [bronze].[crm_prd_info];

		PRINT '>> Inserting data into: crm_prd_info';
		BULK INSERT [bronze].[crm_prd_info]
		FROM 'C:\Users\Sivananda\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milli seconds';
		PRINT '------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting data into: crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Sivananda\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milli seconds';

		PRINT '------------------------------';

		PRINT '------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting data into: erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Sivananda\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milli seconds';
		PRINT '------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting data into: erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Sivananda\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milli seconds';
		PRINT '------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting data into: erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Sivananda\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milli seconds';
		PRINT '------------------------------';

		PRINT '---------------------==============================-------------------------';
		PRINT '                   Loading Bronze Layer is completed';
		SET @batch_end_time = GETDATE();
		PRINT '				   Total Duration: ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' milli seconds';
		PRINT '---------------------==============================-------------------------';
	END TRY
	BEGIN CATCH
		PRINT '============================='
		PRINT 'ERROR LOADING DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR NUMBER: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '============================='
	END CATCH
END
