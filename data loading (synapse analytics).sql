USE GoldAnalytics;
GO

CREATE OR ALTER VIEW dim_customer_360 AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_customer_360/*.parquet',
    DATA_SOURCE = 'GoldStorageSource',
    FORMAT = 'PARQUET'
) AS [marts];
GO

SELECT TOP 10 * FROM dim_customer_360;


CREATE OR ALTER VIEW fct_banking_transactions AS
SELECT *
FROM OPENROWSET(
    BULK 'fct_banking_transactions/*.parquet',
    DATA_SOURCE = 'GoldStorageSource',
    FORMAT = 'PARQUET'
) AS [marts];
GO

select * from fct_banking_transactions;
