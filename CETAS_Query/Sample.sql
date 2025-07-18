select * from 
OPENROWSET(
    BULK 'https://sabrazilecommerce.blob.core.windows.net/silver/E_Commerce_GeoLocation/',
    FORMAT = 'PARQUET'
) as query1