--------------------
-- Create master Key
--------------------
CREATE MASTER KEY ENCRYPTION BY PASSWORD ='Abcd1234**';

------------------
-- Database scope credentials
------------------
CREATE DATABASE SCOPED CREDENTIAL cred_Ecomm
WITH
    IDENTITY='Managed Identity' ;

------------------
-- CREATE External Data Source
------------------
---- For silver Layer
CREATE EXTERNAL DATA SOURCE Ecomm_silver
WITH
(
    LOCATION='https://sabrazilecommerce.blob.core.windows.net/silver',
    CREDENTIAL = cred_Ecomm
)
---- For Gold Layer
CREATE EXTERNAL DATA SOURCE Ecomm_gold
WITH
(
    LOCATION='https://sabrazilecommerce.blob.core.windows.net/gold',
    CREDENTIAL = cred_Ecomm
)


------------------
-- CREATE External File Format
------------------
CREATE EXTERNAL FILE FORMAT Ecomm_format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)