--- CETAS is first load the data in ADLS and then created external table
-- CETAS CREATE TABLE AS SELECT

-----------------------
-- 1. CREATE EXTERNAL TABLE EXTGeoLocation
-----------------------

CREATE EXTERNAL TABLE gold.Ext_Location
WITH
(
    LOCATION ='extLocation',  -- ADLS folder location
    DATA_SOURCE = Ecomm_gold,
    FILE_FORMAT = Ecomm_format_parquet
)
AS
SELECT * FROM gold.GeoLocation  -- view called
----- Execute external Table
select * from gold.extlocation

-----------------------
-- 2. CREATE EXTERNAL TABLE Order_Item
-----------------------

CREATE EXTERNAL TABLE gold.Ext_OrderItem
WITH
(
    LOCATION ='extOrderItem',  -- ADLS folder location
    DATA_SOURCE = Ecomm_gold,
    FILE_FORMAT = Ecomm_format_parquet
)
AS
SELECT * FROM gold.OrderItem  -- view called
----- Execute external Table
select * from gold.Ext_OrderItem


-----------------------
-- 3. CREATE EXTERNAL TABLE ORDER REVIEWS
-----------------------

CREATE EXTERNAL TABLE gold.Ext_OrderReviews
WITH
(
    LOCATION ='extOrderReviews',  -- ADLS folder location
    DATA_SOURCE = Ecomm_gold,
    FILE_FORMAT = Ecomm_format_parquet
)
AS
SELECT * FROM gold.OrderReviews  -- view called
----- Execute external Table
select * from gold.Ext_OrderReviews

-----------------------
-- 4. CREATE EXTERNAL TABLE PRODUCTS
-----------------------

CREATE EXTERNAL TABLE gold.Ext_Products
WITH
(
    LOCATION ='extProducts',  -- ADLS folder location
    DATA_SOURCE = Ecomm_gold,
    FILE_FORMAT = Ecomm_format_parquet
)
AS
SELECT * FROM gold.Products  -- view called
----- Execute external Table
select * from gold.Ext_Products


-----------------------
-- 5. CREATE EXTERNAL TABLE SELLERS
-----------------------

CREATE EXTERNAL TABLE gold.Ext_Sellers
WITH
(
    LOCATION ='extSellers',  -- ADLS folder location
    DATA_SOURCE = Ecomm_gold,
    FILE_FORMAT = Ecomm_format_parquet
)
AS
SELECT * FROM gold.Sellers  -- view called
----- Execute external Table
select * from gold.Ext_Sellers

-----------------------
-- 6. CREATE EXTERNAL TABLE CUSTOMER 
-----------------------

CREATE EXTERNAL TABLE gold.Ext_Customer
WITH
(
    LOCATION ='extCustomer',  -- ADLS folder location
    DATA_SOURCE = Ecomm_gold,
    FILE_FORMAT = Ecomm_format_parquet
)
AS
SELECT * FROM gold.Customer -- view called
----- Execute external Table
select * from gold.Ext_Customer


-----------------------
-- 7. CREATE EXTERNAL TABLE ORDER PAYMENTS  
-----------------------

CREATE EXTERNAL TABLE gold.Ext_OrderPayments
WITH
(
    LOCATION ='extOrderPayments',  -- ADLS folder location
    DATA_SOURCE = Ecomm_gold,
    FILE_FORMAT = Ecomm_format_parquet
)
AS
SELECT * FROM gold.OrderPayments-- view called
----- Execute external Table
select * from gold.Ext_OrderPayments


-----------------------
-- 8. CREATE EXTERNAL TABLE ORDERS
-----------------------

CREATE EXTERNAL TABLE gold.Ext_Orders
WITH
(
    LOCATION ='extOrders',  -- ADLS folder location
    DATA_SOURCE = Ecomm_gold,
    FILE_FORMAT = Ecomm_format_parquet
)
AS
SELECT * FROM gold.Orders -- view called
----- Execute external Table
select * from gold.Ext_Orders

-----------------------
-- 9. CREATE EXTERNAL TABLE PRODUCT CATEGORY
-----------------------

CREATE EXTERNAL TABLE gold.Ext_ProductCategory
WITH
(
    LOCATION ='extProductCategory',  -- ADLS folder location
    DATA_SOURCE = Ecomm_gold,
    FILE_FORMAT = Ecomm_format_parquet
)
AS
SELECT * FROM gold.ProductCategory -- view called
----- Execute external Table
select * from gold.Ext_ProductCategory
