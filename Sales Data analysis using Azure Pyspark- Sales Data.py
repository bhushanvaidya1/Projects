# Databricks notebook source
# DBTITLE 1,Sales dataframe
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

schema1= StructType([
            StructField("Product_id", IntegerType(),True),
            StructField("Customer_id", StringType(),True),
            StructField("Order_date", DateType(),True),
            StructField("Location", StringType(),True),
            StructField("Source_order", StringType(),True)
])

sales_df = spark.read.option("header","true").schema(schema1).format("csv").load("dbfs:/FileStore/tables/pyspark_project/sales_csv__1_.txt")
sales_df.display()

sales_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Derive YEAR,  MONTH , QUAERTER
from pyspark.sql.functions import month,year,quarter,col

sales_df = sales_df.withColumn("Order_year", year(col("Order_date"))) \
                    .withColumn("Order_Month",month(col("Order_date"))) \
                    .withColumn("Order_quarter",quarter(col("Order_date")))
sales_df.display()

# COMMAND ----------

# DBTITLE 1,Menu dataframe
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

schema2= StructType([
            StructField("Product_id", IntegerType(),True),
            StructField("Product_name", StringType(),True),
            StructField("Price", StringType(),True)
])

menu_df = spark.read.option("header","true").schema(schema2).format("csv").load("dbfs:/FileStore/tables/pyspark_project/menu_csv__1_.txt")
menu_df.display()

menu_df.printSchema()

# COMMAND ----------

# DBTITLE 1,inner join sales and menu df
# Perform inner join between sales_df and menu_df on Product_id
sales_menu_inner = sales_df.join(menu_df, sales_df.Product_id == menu_df.Product_id, "inner") 

sales_menu_inner.display()

# COMMAND ----------

# DBTITLE 1,Total amount spend by each customer
from pyspark.sql.functions import col, asc

total_amount_spend_each_customer = sales_menu_inner \
                        .groupBy("Customer_id").agg({"Price":"sum"}) \
                        .withColumnRenamed("sum(Price)", "Total_amount_Spent") \
                        .orderBy(asc("Customer_id"))  # usage of asc for ordering


total_amount_spend_each_customer.display()

# COMMAND ----------

# DBTITLE 1,Total amount spend by each food category
from pyspark.sql.functions import col, asc

total_amount_spend_each_food_category= sales_menu_inner \
                        .groupBy("Product_name").agg({"Price":"sum"}) \
                        .withColumnRenamed("sum(Price)", "Total_amount_Spent_category") \
                        .orderBy(asc("Product_name"))  # usage of asc for ordering


total_amount_spend_each_food_category.display()

# COMMAND ----------

# DBTITLE 1,Total amount of sales each month
from pyspark.sql.functions import col, asc

total_amount_sales_each_month= sales_menu_inner \
                        .groupBy("Order_Month").agg({"Price":"sum"}) \
                        .withColumnRenamed("sum(Price)", "Total amount of sales each month") \
                        .orderBy(asc("Order_Month"))  #  usage of asc for ordering


total_amount_sales_each_month.display()

# COMMAND ----------

# DBTITLE 1,Yearly sales
from pyspark.sql.functions import col, asc


yearly_sales= sales_menu_inner \
                        .groupBy("Order_year").agg({"Price":"sum"}) \
                        .withColumnRenamed("sum(Price)", "Yearly sales") \
                        .orderBy(asc("Order_year"))  # usage of asc for ordering


yearly_sales.display()

# COMMAND ----------

# DBTITLE 1,Quartly sales
from pyspark.sql.functions import col, asc

Quartly_sales= sales_menu_inner \
                        .groupBy("Order_quarter").agg({"Price":"sum"}) \
                        .withColumnRenamed("sum(Price)", "Quartly_sales") \
                        .orderBy(asc("Order_quarter"))  # Correct usage of asc for ordering


Quartly_sales.display()

# COMMAND ----------

# DBTITLE 1,How many time each product puechase
from pyspark.sql.functions import col, count,desc

# Perform inner join between sales_df and menu_df on Product_id
purchase_df = sales_menu_inner \
    .groupBy(sales_df["Product_id"], menu_df["Product_name"]) \
    .agg(count(sales_df["Product_id"]).alias("Product_count")) \
    .orderBy(col("Product_count").desc()) \
    .drop(sales_df["Product_id"])  # Drop the Product_id column

# Display the result
purchase_df.display()


# COMMAND ----------

# DBTITLE 1,Top 4 order items
from pyspark.sql.functions import col, count, desc

order_items = sales_menu_inner \
    .groupBy(sales_df["Product_id"], menu_df["Product_name"]) \
    .agg(count(sales_df["Product_id"]).alias("Product_count")) \
    .orderBy(col("Product_count").desc()) \
    .drop(sales_df["Product_id"])  # Drop the Product_id column
order_items = order_items.limit(4)  # Limit to top 4 items

# Display the result
order_items.display()


# COMMAND ----------

# DBTITLE 1,Top order itesm
from pyspark.sql.functions import col, count, desc

top_order_items = sales_menu_inner \
    .groupBy(sales_df["Product_id"], menu_df["Product_name"]) \
    .agg(count(sales_df["Product_id"]).alias("Product_count")) \
    .orderBy(col("Product_count").desc()) \
    .drop(sales_df["Product_id"])  # Drop the Product_id column
top_order_items = order_items.limit(1)  # Limit to top 4 items

# Display the result
top_order_items.display()


# COMMAND ----------

# DBTITLE 1,Frequency of customer visited to restaurant
from pyspark.sql.functions import countDistinct, col

# Filter orders with source as 'Restaurant', group by Customer_id, and count distinct order dates
df = sales_df.filter(col("Source_order") == "Restaurant") \
             .groupBy("Customer_id") \
             .agg(countDistinct("Order_date").alias("Unique_Order_Count"))

df.display()


# COMMAND ----------

# DBTITLE 1,Total sales by each country
from pyspark.sql.functions import col, asc

# Group by Location, sum the Price, rename the result, and order by Location
total_amount_spend_each_country = sales_menu_inner \
                        .groupBy("Location") \
                        .agg({"Price": "sum"}) \
                        .withColumnRenamed("sum(Price)", "Total_amount_spend_country") \
                        .orderBy(asc("Location"))  # ordering by Location

total_amount_spend_each_country.display()


# COMMAND ----------

# DBTITLE 1,Total sales by order source
from pyspark.sql.functions import col, asc

# Group by Source_order, sum the Price, rename the result, and order by Source_order
total_sales_by_order_source = sales_menu_inner \
                        .groupBy("Source_order") \
                        .agg({"Price": "sum"}) \
                        .withColumnRenamed("sum(Price)", "Total_sales_by_source_order") \
                        .orderBy(asc("Source_order"))  # ordering by Source_order

total_sales_by_order_source.display()

