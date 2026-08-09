USE GoldAnalytics;
GO

IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name LIKE '%DatabaseMasterKey%')
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = '00@N@S00anas!khudabukhsh';
GO

IF NOT EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'SynapseStorageCredential')
CREATE DATABASE SCOPED CREDENTIAL SynapseStorageCredential
WITH IDENTITY = 'Managed Identity';
GO

CREATE EXTERNAL DATA SOURCE GoldStorageSource
WITH (
    LOCATION = 'https://bankingprojectstacc.dfs.core.windows.net/gold',
    CREDENTIAL = SynapseStorageCredential
);
GO


CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
    FORMAT_TYPE = PARQUET
);
GO