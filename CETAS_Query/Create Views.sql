-----------------------
-- 1. CREATE VIEW GEO LOCATION 
-----------------------
CREATE VIEW gold.GeoLocation
As
    select * from 
    OPENROWSET(
    BULK 'https://sabrazilecommerce.blob.core.windows.net/silver/E_Commerce_GeoLocation/',
    FORMAT = 'PARQUET'
)as Query1

-----------------------
-- 2. CREATE VIEW ORDER ITEMS
-----------------------
CREATE VIEW gold.OrderItem
As
    select * from 
    OPENROWSET(
    BULK 'https://sabrazilecommerce.blob.core.windows.net/silver/E_Commerce_Order_Item/',
    FORMAT = 'PARQUET'
)as Query1

-----------------------
-- 3. CREATE VIEW ORDER REVIEWS
-----------------------
CREATE VIEW gold.OrderReviews
As
    select * from 
    OPENROWSET(
    BULK 'https://sabrazilecommerce.blob.core.windows.net/silver/E_Commerce_Order_Reviews/',
    FORMAT = 'PARQUET'
)as Query1


-----------------------
-- 4. CREATE VIEW PRODUCTS
-----------------------
CREATE VIEW gold.Products
As
    select * from 
    OPENROWSET(
    BULK 'https://sabrazilecommerce.blob.core.windows.net/silver/E_Commerce_Products/',
    FORMAT = 'PARQUET'
)as Query1


-----------------------
-- 5. CREATE VIEW SELLERS
-----------------------
CREATE VIEW gold.Sellers
As
    select * from 
    OPENROWSET(
    BULK 'https://sabrazilecommerce.blob.core.windows.net/silver/E_Commerce_Sellers/',
    FORMAT = 'PARQUET'
)as Query1

-----------------------
-- 6. CREATE VIEW CUSTOMER 
-----------------------
CREATE VIEW gold.Customer
As
    select * from 
    OPENROWSET(
    BULK 'https://sabrazilecommerce.blob.core.windows.net/silver/E_Commerce_customers/',
    FORMAT = 'PARQUET'
)as Query1

-----------------------
-- 7. CREATE VIEW ORDER PAYMENT 
-----------------------
CREATE VIEW gold.OrderPayments
As
    select * from 
    OPENROWSET(
    BULK 'https://sabrazilecommerce.blob.core.windows.net/silver/E_Commerce_order_payments/',
    FORMAT = 'PARQUET'
)as Query1

-----------------------
-- 8. CREATE VIEW ORDERS
-----------------------
CREATE VIEW gold.Orders
As
    select * from 
    OPENROWSET(
    BULK 'https://sabrazilecommerce.blob.core.windows.net/silver/E_Commerce_orders/',
    FORMAT = 'PARQUET'
)as Query1

-----------------------
-- 9. CREATE VIEW PRODUCT CATEGORY
-----------------------
CREATE VIEW gold.ProductCategory
As
    select * from 
    OPENROWSET(
    BULK 'https://sabrazilecommerce.blob.core.windows.net/silver/E_Commerce_product_category/',
    FORMAT = 'PARQUET'
)as Query1