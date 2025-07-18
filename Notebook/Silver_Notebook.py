# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # Brazilian E-Commerce (SILVER LAYER SCRIPT)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### DATA ACCESS USING APP (Service Principle)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.sabrazilecommerce.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.sabrazilecommerce.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.sabrazilecommerce.dfs.core.windows.net","d452cfdb-2cbb-4edd-a771-2affe49c8de7") #Application (client) ID 
spark.conf.set("fs.azure.account.oauth2.client.secret.sabrazilecommerce.dfs.core.windows.net", "lnl8Q~4AhBdq4Ya-sSE6MU76FGvox~au3IqqKb1C") #secret value
spark.conf.set("fs.azure.account.oauth2.client.endpoint.sabrazilecommerce.dfs.core.windows.net", "https://login.microsoftonline.com/9d894dd8-f4b4-4748-bc76-99871acb6201/oauth2/token") # Directory (tenant) ID

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### 1. Data Reading into Customer File from bronze container

# COMMAND ----------

df_cus=spark.read.format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load('abfss://bronze@sabrazilecommerce.dfs.core.windows.net/customers')

# COMMAND ----------

df_cus.display()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### Data Transformations for Customer File

# COMMAND ----------

## Drop dupliates
df_cus = df_cus.dropDuplicates()

# COMMAND ----------

## Dropping the null values
df_cus  = df_cus .dropna(subset=["customer_id", "customer_unique_id"])

# COMMAND ----------

###Column Name proper set
##from pyspark.sql.functions import col

df_cus = df_cus.select([col(c).alias(c.lower().replace(" ", "_")) for c in df_cus.columns])



# COMMAND ----------

## Renamed Column
df_cus = df_cus.withColumnRenamed("customer_zip_code_prefix", "customer_zip_code")

# COMMAND ----------

df_cus.display()

# COMMAND ----------

### Create state column by using status Code 

df_cus = df_cus.withColumn(
    "customer_state_full_Name",
     when(col("customer_state") == "AC", "Acre")
    .when(col("customer_state") == "AL", "Alagoas")
    .when(col("customer_state") == "AM", "Amazonas")
    .when(col("customer_state") == "AP", "Amapá")
    .when(col("customer_state") == "BA", "Bahia")
    .when(col("customer_state") == "CE", "Ceará")
    .when(col("customer_state") == "DF", "Distrito Federal")
    .when(col("customer_state") == "ES", "Espírito Santo")
    .when(col("customer_state") == "GO", "Goiás")
    .when(col("customer_state") == "MA", "Maranhão")
    .when(col("customer_state") == "MG", "Minas Gerais")
    .when(col("customer_state") == "MS", "Mato Grosso do Sul")
    .when(col("customer_state") == "MT", "Mato Grosso")
    .when(col("customer_state") == "PA", "Pará")
    .when(col("customer_state") == "PB", "Paraíba")
    .when(col("customer_state") == "PE", "Pernambuco")
    .when(col("customer_state") == "PI", "Piauí")
    .when(col("customer_state") == "PR", "Paraná")
    .when(col("customer_state") == "RJ", "Rio de Janeiro")
    .when(col("customer_state") == "RN", "Rio Grande do Norte")
    .when(col("customer_state") == "RO", "Rondônia")
    .when(col("customer_state") == "RR", "Roraima")
    .when(col("customer_state") == "RS", "Rio Grande do Sul")
    .when(col("customer_state") == "SC", "Santa Catarina")
    .when(col("customer_state") == "SE", "Sergipe")
    .when(col("customer_state") == "SP", "São Paulo")
    .when(col("customer_state") == "TO", "Tocantins")
    .otherwise("Unknown")
)

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.write.format('parquet')\
        .mode('overwrite')\
        .option("path","abfss://silver@sabrazilecommerce.dfs.core.windows.net/E_Commerce_customers")\
        .save()

##.option("compression","snappy")\

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### 2. Data Reading into GeoLocaion File from bronze container

# COMMAND ----------

df_Geo=spark.read.format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load('abfss://bronze@sabrazilecommerce.dfs.core.windows.net/geolocation')

# COMMAND ----------

df_Geo.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC %md
# MAGIC #### Data Transformations for Geo Location File

# COMMAND ----------

## drop duplicates and drop null values
df_Geo = df_Geo.dropDuplicates() 
df_Geo = df_Geo.dropna()

# COMMAND ----------

# # trim all the space in column Name 

#from pyspark.sql.functions import trim

#df_Geo = df_Geo.select([trim(col(c)).alias(c) for c in df_Geo.columns])


df_Geo = df_Geo.withColumnRenamed("geolocation_zip_code_prefix", "zip_code_prefix") \
       .withColumnRenamed("geolocation_lat", "latitude") \
       .withColumnRenamed("geolocation_lng", "longitude") \
       .withColumnRenamed("geolocation_city", "city") \
       .withColumnRenamed("geolocation_state", "state")


# COMMAND ----------

# DataType Changed in existing column 

df_Geo=df_Geo.withColumn("latitude", col('latitude').cast("double"))\
    .withColumn("longitude", col('longitude').cast("double"))

# COMMAND ----------

df_Geo=df_Geo.withColumn("latitude_rounded", round("latitude", 4))\
.withColumn("longitude_rounded", round("longitude", 4))

# COMMAND ----------

## geo_Location Column Created 
df_Geo=df_Geo.withColumn("geo_location",concat_ws(',',col('latitude_rounded'),col('longitude_rounded')))

# COMMAND ----------

df_Geo.display()

# COMMAND ----------

## Aggrigated Date checked
df_Geo.groupBy("zip_code_prefix").count().orderBy("count", ascending=False).show()

# COMMAND ----------

df_Geo.write.format('parquet')\
        .mode('overwrite')\
        .option("path","abfss://silver@sabrazilecommerce.dfs.core.windows.net/E_Commerce_GeoLocation")\
        .save()

##.option("compression","snappy")\

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### `3`. Data Reading into Order Item File from bronze container

# COMMAND ----------

df_ordItm=spark.read.format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load('abfss://bronze@sabrazilecommerce.dfs.core.windows.net/order_items')

# COMMAND ----------

df_ordItm.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

### dropping duplicates and null values
df_ordItm = df_ordItm.dropDuplicates() 
df_ordItm = df_ordItm.dropna()

# COMMAND ----------

#### Rename Columns 
df_ordItm = df_ordItm.withColumnRenamed("order_id", "order_id") \
       .withColumnRenamed("order_item_id", "item_number") \
       .withColumnRenamed("product_id", "product_code") \
       .withColumnRenamed("seller_id", "seller_code") \
       .withColumnRenamed("shipping_limit_date", "shipping_deadline") \
       .withColumnRenamed("price", "item_price") \
       .withColumnRenamed("freight_value", "shipping_cost")

# COMMAND ----------

## Change DataType 
df_ordItm = df_ordItm.withColumn("item_price", col("item_price").cast("double"))\
    .withColumn("shipping_cost", col("shipping_cost").cast("double"))\
    .withColumn("shipping_deadline", col("shipping_deadline").cast("timestamp"))


# COMMAND ----------

## Create New Column "Total Cost" sum of price and cost

df_ordItm = df_ordItm.withColumn("total_cost", col("item_price") + col("shipping_cost"))

# COMMAND ----------


##  Extracting month , year, date from shipping_deadline column
## from pyspark.sql.functions import year, month, dayofmonth

df_ordItm = df_ordItm.withColumn("shipping_year", year("shipping_deadline")) \
       .withColumn("shipping_month", month("shipping_deadline")) \
       .withColumn("shipping_day", dayofmonth("shipping_deadline"))


# COMMAND ----------

### Filer and Aggrigated data

df_ordItm.filter(col("item_price") > 100).display()


df_ordItm.groupBy("seller_code").agg(
    sum("item_price").alias("total_price"),
    sum("shipping_cost").alias("total_freight"),
    avg("item_price").alias("average_price")
).display()

## Aggrigate and pivot 
##df_ordItm.groupBy("product_code").pivot("order_item_id").count().display()

# COMMAND ----------

## Write File in ADLS 
df_ordItm.write.format('parquet')\
        .mode('overwrite')\
        .option("path","abfss://silver@sabrazilecommerce.dfs.core.windows.net/E_Commerce_Order_Item")\
        .save()

# COMMAND ----------

df_ordItm.display()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### `4`. Data Reading into Order Payments File from bronze container

# COMMAND ----------

df_pay=spark.read.format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load('abfss://bronze@sabrazilecommerce.dfs.core.windows.net/order_payments')

# COMMAND ----------

df_pay.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformations 

# COMMAND ----------

### dropping null and duplicate records
df_pay = df_pay.dropna()
df_pay = df_pay.dropDuplicates()

# COMMAND ----------

### Rename Columns for Readability
df_pay = df_pay.withColumnRenamed("order_id", "order_id") \
       .withColumnRenamed("payment_sequential", "payment_sequence") \
       .withColumnRenamed("payment_type", "payment_method") \
       .withColumnRenamed("payment_installments", "installment_count") \
       .withColumnRenamed("payment_value", "payment_amount")

# COMMAND ----------


## Data Type Change 

df_pay = df_pay.withColumn("payment_amount", col("payment_amount").cast("double")) \
       .withColumn("installment_count", col("installment_count").cast("int")) \
       .withColumn("payment_sequence", col("payment_sequence").cast("int"))

# COMMAND ----------

df_pay.filter(col("payment_amount") > 100).display()


df_pay.groupBy("order_id").agg(
    {"payment_amount": "sum", "installment_count": "sum"}
).withColumnRenamed("sum(payment_amount)", "total_payment") \
 .withColumnRenamed("sum(installment_count)", "total_installments")


# COMMAND ----------

## Write File in ADLS 
df_pay.write.format('parquet')\
        .mode('overwrite')\
        .option("path","abfss://silver@sabrazilecommerce.dfs.core.windows.net/E_Commerce_order_payments")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### `5`. Data Reading into orders File from bronze container

# COMMAND ----------

df_Ord=spark.read.format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load('abfss://bronze@sabrazilecommerce.dfs.core.windows.net/orders')

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### Transformations 

# COMMAND ----------

# Convert date columns
date_columns = ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
                "order_delivered_customer_date", "order_estimated_delivery_date"]  # List of columns

for col_name in date_columns:
    df_Ord = df_Ord.withColumn(col_name, col(col_name).cast(TimestampType()))

# COMMAND ----------


# Drop rows with null order_id or order_status
df_Ord = df_Ord.dropna(subset=["order_id", "order_status"])

# COMMAND ----------

# Extract date parts
df_Ord = df_Ord.withColumn("purchase_year", year("order_purchase_timestamp")) \
       .withColumn("purchase_month", month("order_purchase_timestamp")) \
       .withColumn("purchase_day", dayofmonth("order_purchase_timestamp"))

# COMMAND ----------

# Calculate delivery duration
df_Ord = df_Ord.withColumn("delivery_days", datediff("order_delivered_customer_date", "order_purchase_timestamp"))

# COMMAND ----------

### Column Created Based on date time

df_Ord = df_Ord.withColumn("actual_delivery_days",
                   datediff("order_delivered_customer_date", "order_purchase_timestamp"))

df_Ord = df_Ord.withColumn("estimated_delivery_days",
                   datediff("order_estimated_delivery_date", "order_purchase_timestamp"))

df_Ord = df_Ord.withColumn("delay_in_delivery",
                   datediff("order_delivered_customer_date", "order_estimated_delivery_date"))

df_Ord = df_Ord.withColumn("is_late",
                   when(col("delay_in_delivery") > 0, 1).otherwise(0))

# Flag late deliveries
#df_Ord = df_Ord.withColumn("is_late", when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"), 1).otherwise(0))

# COMMAND ----------

### Date is null then Remove records

df_Ord = df_Ord.filter(
    col("order_purchase_timestamp").isNotNull() &
    col("order_delivered_customer_date").isNotNull() &
    col("order_estimated_delivery_date").isNotNull()
)

# COMMAND ----------

df_Ord.display()

# COMMAND ----------

## Write File in ADLS 
df_Ord.write.format('parquet')\
        .mode('overwrite')\
        .option("path","abfss://silver@sabrazilecommerce.dfs.core.windows.net/E_Commerce_orders")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### `6`. Data Reading into Products File from bronze container

# COMMAND ----------

df_prod=spark.read.format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load('abfss://bronze@sabrazilecommerce.dfs.core.windows.net/products')

# COMMAND ----------

df_prod.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformations 

# COMMAND ----------

## Define List for column names to changing the data type

int_columns = [
    "product_name_lenght", "product_description_lenght", "product_photos_qty",
    "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
]

for c in int_columns:
    df_prod = df_prod.withColumn(c, col(c).cast(IntegerType()))

# COMMAND ----------

##Added space and remove "_" field in product_category_name

df_prod = df_prod.withColumn("product_category_name",
                   regexp_replace(lower(col("product_category_name")), "_", " "))

# COMMAND ----------

## Calcuate Product volumne

df_prod = df_prod.withColumn("product_volume_cm3",
                   col("product_length_cm") * col("product_height_cm") * col("product_width_cm"))

# COMMAND ----------

### identify product is Heavy or light weight bifercate with 2 columns

df_prod = df_prod.withColumn("is_bulky", when(col("product_volume_cm3") > 10000, 1).otherwise(0)) \
       .withColumn("is_lightweight", when(col("product_weight_g") < 500, 1).otherwise(0))

# COMMAND ----------

## Average Dimention by category column

df_prod.groupBy("product_category_name").agg(
    {"product_weight_g": "avg", "product_volume_cm3": "avg", "desc_to_name_ratio": "avg"}
)

# COMMAND ----------

df_prod.display()

# COMMAND ----------

## Write File in ADLS 
df_prod.write.format('parquet')\
        .mode('overwrite')\
        .option("path","abfss://silver@sabrazilecommerce.dfs.core.windows.net/E_Commerce_Products")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### `7`. Data Reading into Seller File from bronze container

# COMMAND ----------

df_Sell=spark.read.format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load('abfss://bronze@sabrazilecommerce.dfs.core.windows.net/sellers')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformations 

# COMMAND ----------

## Data Type Changed
df_Sell = df_Sell.withColumn("seller_zip_code_prefix", col("seller_zip_code_prefix").cast(IntegerType()))

# COMMAND ----------


### upper and lower letter for city and State 
df_Sell = df_Sell.withColumn("seller_city", trim(lower(col("seller_city")))) \
       .withColumn("seller_state", upper(trim(col("seller_state"))))

# COMMAND ----------

# Checking duplicates 
df_Sell.groupBy("seller_id").count().filter("count > 1").show()

# COMMAND ----------

df_Sell.display()

# COMMAND ----------

## Write File in ADLS 
df_Sell.write.format('parquet')\
        .mode('overwrite')\
        .option("path","abfss://silver@sabrazilecommerce.dfs.core.windows.net/E_Commerce_Sellers")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### `8`. Data Reading into Product_Category File from bronze container

# COMMAND ----------

df_cat=spark.read.format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load('abfss://bronze@sabrazilecommerce.dfs.core.windows.net/product_category')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformations 

# COMMAND ----------

## trim and lower letter for category name and english category name
df_cat = df_cat.withColumn("product_category_name", trim(lower(col("product_category_name")))) \
               .withColumn("product_category_name_english", trim(lower(col("product_category_name_english"))))

# COMMAND ----------

# Duplicate Drop basd on product_category_name
df_cat = df_cat.dropDuplicates(["product_category_name"])

# COMMAND ----------

## Write File in ADLS 
df_cat.write.format('parquet')\
        .mode('overwrite')\
        .option("path","abfss://silver@sabrazilecommerce.dfs.core.windows.net/E_Commerce_product_category")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### `9`. Data Reading into Order Reviews File from bronze container

# COMMAND ----------

df_rev=spark.read.format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load('abfss://bronze@sabrazilecommerce.dfs.core.windows.net/order_reviews')

# COMMAND ----------

df_rev.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformations 
# MAGIC

# COMMAND ----------

### Data Type Changed

##df_rev = df_rev.withColumn("review_creation_date", col("review_creation_date").cast(TimestampType())) \
##       .withColumn("review_answer_timestamp", col("review_answer_timestamp").cast(TimestampType()))

# COMMAND ----------

### create new column with using review Score

df_rev = df_rev.withColumn(
    "review_category",
    when(col("review_score") == 5, "positive")
    .when(col("review_score") == 4, "neutral")
    .otherwise("negative")
)

# COMMAND ----------

### If comment title is null then 0 else 1

df_rev = df_rev.withColumn(
    "has_comment_title",
    when(col("review_comment_title").isNotNull(), 1).otherwise(0)
)

# COMMAND ----------

df_rev = df_rev.fillna({"review_comment_title": "No comment"})

# COMMAND ----------


### Aggigate with Review Score
df_rev.groupBy("review_score").count().orderBy("review_score").show()

# COMMAND ----------

df_rev.display()

# COMMAND ----------

## Write File in ADLS 
df_rev.write.format('parquet')\
        .mode('overwrite')\
        .option("path","abfss://silver@sabrazilecommerce.dfs.core.windows.net/E_Commerce_Order_Reviews")\
        .save()