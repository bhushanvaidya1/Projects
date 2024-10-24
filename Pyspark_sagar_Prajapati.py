# Databricks notebook source
# MAGIC %md
# MAGIC # How to read a CSV file

# COMMAND ----------

df = spark.read.option("Header","True").option("Inferschema","True").format("csv").load("dbfs:/FileStore/tables/A/customer_shopping_data.csv")
df.display()
df.printSchema()
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # How to Rename columns in DataFrame

# COMMAND ----------

# Sample DataFrame
data = [("Alice", 29,30000), ("Bob", 35,25000)]
df_R = spark.createDataFrame(data, ["Name", "Age","Salary"])
df_R.display()

# Renaming 'age' to 'years'
df_renamed = df_R.withColumnRenamed("Age", "years").withColumnRenamed("Salary","Amount")
df_renamed.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC select

# COMMAND ----------

# MAGIC %md
# MAGIC #How to ADD New Columns in DataFrame 

# COMMAND ----------

from pyspark.sql.functions import col, lit
df1 = df.withColumn("Country",lit("India")).withColumn("Total Bill",(col("quantity")*col("price")))
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # How to filter a DataFrame 

# COMMAND ----------

df.display()
df_filter= df.filter((col("Id")==1) & (col("Age")==20))
df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # How to Sort a Dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC # sort

# COMMAND ----------

data  = [(1,'sagar', 20,"India"), \
    (2,'Shivam',23,'India'), \
        (3,"Muni",26,"India") ,\
            (1,"sagar",30,"India")    
    ]
columns = ["Id","Name","Age","Country"]
df_s =spark.createDataFrame(data =data, schema= columns)
df.display()
df_sort= df.sort(col("Id").desc())
df_sort.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # OrderBy

# COMMAND ----------


df2_orderBy =df1.orderBy(col("Id").desc(),col("Age").desc())
df2_orderBy.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # How to remove Duplicates in DataFrame

# COMMAND ----------

data  =[(1,'sagar',20),\
    (2,'shivam',23),\
        (3,'Muni',26),\
            (1,'sagar',20),\
                (1,'sagar',40)
                ]     
colums = ['Id','Name','Age']
df_duplicate  = spark.createDataFrame(data=data, schema= colums)
df_duplicate.display()           

# COMMAND ----------

# MAGIC %md
# MAGIC # distinct

# COMMAND ----------

df_duplicate1 =df_duplicate.distinct()  # for all columns , not specific columns
df_duplicate1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # dropDuplicates

# COMMAND ----------

df_duplicate2 =df_duplicate.dropDuplicates(['Id','Name'])  # for provide specific columns   and  # for all columns
df_duplicate2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # How to use GroupBY in DataFrame

# COMMAND ----------

data  = [(1,'sagar',50),\
    (2,'shivam',40),\
        (3,'Muni',20),\
            (1,'sagar',80),\
                (2,'shiavm',10),\
                    (3,'Muni',90)
    ]
columns =['Id','Name','Marks']
df_g=spark.createDataFrame(data=data, schema=columns)
df_g.display()

# COMMAND ----------

# groupBy ----> compulsory to use aggregate function with groupBy

# multiple agg.function cannot use in one dataframe/ one statement----->>> use agg function for that

Df_groupBy  = df_g.groupBy("Id").max("Marks")
Df_groupBy.display()

# COMMAND ----------

# DBTITLE 1,cnewufhhfrr
Df_groupBy_1 =df_g.groupBy("Id").sum("Marks")
Df_groupBy_1.display()

# COMMAND ----------

from pyspark.sql.functions import max, min,sum

df_duplicate3 = df_g.groupBy("Id","Name").agg(max("Marks").alias("Max_Marks"),min("Marks").alias("Min_Marsk"),sum("Marks").alias("Total_Marks"))
df_duplicate3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # How to write into CSV 

# COMMAND ----------

# DBTITLE 1,Location save
# df.write.mode("overwrite").csv("path/to/output/overwrite")  # Overwrites any existing data
# df.write.mode("append").csv("path/to/output/append")  # Appends to existing data
# df.write.mode("ignore").csv("path/to/output/ignore")  # Ignores if data exists
# df.write.mode("error").csv("path/to/output/error")  # Throws an error if data exists

df_duplicate3.write.mode("overwrite").csv("/FileStore/tables/output/file1.csv")

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/output/file1.csv/part-00000-tid-9134711868450568823-eabac553-fd4d-4e16-9f83-85476898ee49-104-1-c000.csv")
# Display the DataFrame using Databricks display function
df.display()  # This works in Databricks notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC # How to merge two DataFrame using PySpark

# COMMAND ----------

simpleData = [("sagar","CSE","UP",80), \
    ("shivam","IT","MP",86) \
]
columns = ["StudentName","Department","City","Marks"]
df_A = spark.createDataFrame(data=simpleData,schema=columns)
df_A.show()

# COMMAND ----------

simpleData = [("sagar","CSE","UP",80), \
    ("Mini","Mech","AP",70) \
]
columns = ["StudentName","Department","City","Marks"]
df_B = spark.createDataFrame(data=simpleData,schema=columns)
df_B.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Union

# COMMAND ----------

# No of columns and data_types ---------> same

df_A.union(df_B).show()   # Give duplicate records
df_A.union(df_B).distinct().show()   # to remove duplicate records


# COMMAND ----------

# MAGIC %md
# MAGIC # UnionAll

# COMMAND ----------

df_c =df_A.unionAll(df_B).show()      # Give duplicate records

df_c1 =df_A.unionAll(df_B).distinct()  # to remove duplicate records
df_c1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # How to use WHEN Otherwise in PySpark

# COMMAND ----------

df_c1.display()

from pyspark.sql.functions import when,col

df_4 = df_c1 .withColumn("State",when(col("City")=="UP","Uttar Pradesh").when(col("City")=="MP","Madhya Pradesh").otherwise("Unknown"))
df_4.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # How to join two DataFrames in PySpark - Joins

# COMMAND ----------

simpleData = [(1,"sagar","CSE","UP",80), \
    (2,"Shivam","IT","MP",86), \
    (3,"Mini","Mech","AP",70) \
]
columns = ["ID","Student_Name","Department_Name","City","Marks"]
df_1 = spark.createDataFrame(data=simpleData,schema=columns)
df_1.display()

# COMMAND ----------

simpleData_2 = [(1,"sagar","CSE","UP",80), \
    (3,"Mini","Mech","AP",70) \
]
columns = ["ID","StudentName","Department","City","Marks"]
df_2 = spark.createDataFrame(data=simpleData_2,schema=columns)
df_2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # df1.join(df2, join_condition, join_type)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ***Common Types of Joins 
# MAGIC 1. Inner Join 
# MAGIC 2. Left Join (Left Outer Join)
# MAGIC 3. Right Join (Right Outer Join)
# MAGIC 4. Full Join (Full Outer Join) 
# MAGIC 5. Cross Join >>>>>>>> No condition require , give all possible combinations
# MAGIC 6. Self Join

# COMMAND ----------

# MAGIC %md
# MAGIC ***Join Type	Description
# MAGIC 1. Inner	-Returns rows with matching values in both DataFrames 
# MAGIC
# MAGIC 2. Left	  -Returns all rows from the left DataFrame and matching rows from the right 
# MAGIC
# MAGIC 3. Right	-Returns all rows from the right DataFrame and matching rows from the left 
# MAGIC
# MAGIC 4. Full	  -Returns all rows from both DataFrames, with NULLs where no match is found
# MAGIC
# MAGIC 5. Cross	-Returns the Cartesian product of two DataFrames
# MAGIC  
# MAGIC 6. Self	  -Joins the DataFrame with itself for comparison of rows

# COMMAND ----------

df_join_inner= df_1.join(df_2, df_1.ID==df_2.ID,"inner")
df_join_inner.display()

# COMMAND ----------

df_join_left= df_1.join(df_2, df_1.ID==df_2.ID,"left")
df_join_left.display()

# COMMAND ----------

df_join_right= df_1.join(df_2, df_1.ID==df_2.ID,"right")
df_join_right.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Other way -----> write inner join---alias
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

df_join_inner1= df_1.alias("A").join(df_2.alias("B"), col("A.ID")==col("B.ID"),"inner")  # all columns 
df_join_inner1.display()


# COMMAND ----------

# inner join with specific column 

df_join_inner2 = df_1.alias("A") \
    .join(df_2.alias("B"), col("A.Id") == col("B.Id"), "inner") \
    .select(
        col("A.Id").alias("ID_A"),
        col("B.StudentName").alias("StudentName_B"),
        col("B.Department").alias("Department_B"))
df_join_inner2.display()

# COMMAND ----------

df_cross = df_1.crossJoin(df_2)
df_cross.display()

# COMMAND ----------

# Perform full outer join between df_1 and df_2
df_full_outer_join = df_1.join(df_2, df_1.ID == df_2.ID, "full_outer")

# Display the result of the full outer join
df_full_outer_join.display()


# COMMAND ----------

# MAGIC %md
# MAGIC # How to use Window Fuctions in PySpark
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Example 1

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("Window Functions Example").getOrCreate()

# Sample data
data = [(1, "Alice", 100),
        (2, "Bob", 150),
        (3, "Cathy", 200),
        (4, "David", 150),
        (5, "Eve", 300)]
columns = ["ID", "Name", "Score"]

# Create DataFrame
df = spark.createDataFrame(data, schema=columns)

# Define window specification
window_spec = Window.orderBy(F.col("Score").desc())

# Applying ranking functions
df_ranked = df.withColumn("Rank", F.rank().over(window_spec)) \
              .withColumn("Dense_Rank", F.dense_rank().over(window_spec)) \
              .withColumn("Row_Number", F.row_number().over(window_spec))

df_ranked.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Example 2
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

df_union = df_1.union(df_2)
win = Window.partitionBy("ID").orderBy("Marks")
df_Row = df_union.withColumn("R1",row_number().over(win))
df_Row.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
df_union = df_1.union(df_2)
win = Window.partitionBy("ID").orderBy(col("Marks").asc())
df_Row = df_union.withColumn("R1",row_number().over(win))
df_Row.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
df_union = df_1.union(df_2)
win = Window.partitionBy().orderBy("Marks")
df_Row = df_union.withColumn("R1",row_number().over(win))
df_Row.display()

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Union of two DataFrames
df_union = df_1.union(df_2)

# Define the Window specification (partition by "ID" and order by "Marks")
win = Window.partitionBy("ID").orderBy("Marks")

# Apply the row_number() window function to create a new column "R1"
df_with_row_number = df_union.withColumn("R1", row_number().over(win))

# Select the required columns
df_selected = df_with_row_number.select("ID", "Student_Name", "Department_Name", "R1")

# Display the DataFrame (use display() in Databricks, show() in other environments)
df_selected.display()


# COMMAND ----------

# MAGIC %md
# MAGIC # How to write Dataframe with Partitions using PartitionBy

# COMMAND ----------

# MAGIC %md
# MAGIC # PartionBy

# COMMAND ----------

df_union.display()
df_union.write.option("header","true").partitionBy("ID",'City').mode("overwrite").csv("/FileStore/tables/sample_partition_output")

# COMMAND ----------

df_output = spark.read.option('header','true').csv('/FileStore/tables/sample_partition_output')
df_output.display()

# COMMAND ----------

df_output_2= spark.read.option('header','true').csv('/FileStore/tables/sample_partition_output/ID=2')
df_output_2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Why to use Repartition Method in PySpark 

# COMMAND ----------

bhu.display()
bhu.count()
bhu.repartition(119).write.option('header','true').partitionBy('gender').mode('overwrite').csv('/FileStore/tables/b_out')

# COMMAND ----------

df_union.display()


# COMMAND ----------

# MAGIC %md
# MAGIC # How to create UDF in PySpark 

# COMMAND ----------

def convertcase(st):
    c=''
    for i in st:
        if 'a'<=i<='z':
            c=c+i.upper()
        else:
            c=c+i.lower()
    return c

# COMMAND ----------

from pyspark.sql.functions import col,udf,StringType
df_convetcase = udf(convertcase)
con_1 = df_union.select(col('ID'), df_convetcase(col("Student_Name")).alias("Converted_Student_Name"))
con_1.display()


# COMMAND ----------

# MAGIC %md
# MAGIC # How to do casting of Columns in PySpark 

# COMMAND ----------

from pyspark.sql.types import StringType, StructType, StructField

# Define the schema with all columns as StringType
schema = StructType([
    StructField("Id", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Salary", StringType(), True)
])

# Create the data with null values in the "Salary" column
data = [
    ("1", "Alice", None),
    ("2", "Bob", "5000"),
    ("3", "Charlie", None),
    ("4", "David", "7000"),
    ("5", "Eve", None)
]

# Create the DataFrame
df = spark.createDataFrame(data, schema=schema)

# Display the DataFrame
df.display()
df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC # casting ------> chaange column data type

# COMMAND ----------

# MAGIC %md
# MAGIC using -----> withColumn 

# COMMAND ----------

# Correct the syntax for casting columns
df_casting = df.withColumn("Id", df["Id"].cast("int")) \
               .withColumn("Salary", df["Salary"].cast("int"))

# Print the schema to verify the changes
df_casting.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC using-----------> select

# COMMAND ----------

from pyspark.sql.functions import col

# Select the columns and cast "Id" and "Salary" to IntegerType
df_casting_select = df.select(
    col("Id").cast("int").alias("Id"), 
    col("Name"), 
    col("Salary").cast("int").alias("Salary")
)

# Print the schema to verify the changes
df_casting_select.printSchema()

# Display the DataFrame
df_casting_select.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # How to handle NULLs in PySpark

# COMMAND ----------

data_1 = [(1,'shivam',2800), \
        (2,'sagar',None), \
        (3,None,3000),\
        (4,'Raj',6000), \
        (5,'Kunal',None)
        ]
schema_1 = ["Id",'Name','Salary']
new_df = spark.createDataFrame(data=data_1,schema =schema_1)
new_df.display()

# COMMAND ----------

new_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # df.na.fill

# COMMAND ----------

new = new_df.na.fill("Unknown",'Name')   # name column----> unknown fill---> when find null
new.display()

# COMMAND ----------

new1 = new_df.na.fill("None")   # all string column -------> fill none when find null
new1.display()

# COMMAND ----------

new1 = new_df.na.fill(0)  # all integer column ---->  fill 0 when find null 
new1.display()

# COMMAND ----------

new2 = new_df.na.fill(0,["Id","Salary"])  # specific column integer----->>> fill 0 when find null
new2.display()

# COMMAND ----------

new3= new_df.na.fill({
    "Name": "Unknown",    # Fill missing names with 'Unknown'    # multiple column---->> diff dat types---->>> string, int
    "Salary": 0           # Fill missing salaries with 0
})
new3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # How to pivot a DataFrame in PySpark 

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("Fruits DataFrame Example").getOrCreate()

# Sample data with duplicates in 'fruits' and 'country', and distinct 'price' values in the range of 1000
data = [
    ('Apple', 1000, 'USA'),
    ('Banana', 1500, 'USA'),
    ('Orange', 2000, 'USA'),
    ('Grapes', 2500, 'Mexico'),
    ('Mango', 3000, 'Mexico'),
    ('Banana', 1500, 'Canada'),  # Duplicate fruit, different country
    ('Apple', 1000, 'Canada'),    # Duplicate fruit, different country
    ('Orange', 2000, 'USA'),      # Duplicate fruit, same country
    ('Peach', 1800, 'USA'),
    ('Pineapple', 1700, 'Philippines'),
    ('Grapes', 2500, 'Mexico'),   # Duplicate fruit, same country
    ('Strawberry', 2200, 'USA'),
    ('Apple', 1000, 'USA'),        # Duplicate fruit, same country
    ('Mango', 3000, 'Brazil'),     # Duplicate fruit, different country
    ('Kiwi', 3300, 'New Zealand'),
    ('Peach', 1800, 'Australia'),  # Duplicate fruit, different country
]

# Define schema
schema = ["fruits", "price", "country"]

# Create the DataFrame
fruits_df = spark.createDataFrame(data, schema)

# Show the DataFrame
fruits_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC here groupBy is used 

# COMMAND ----------

fruits_df_pivot = fruits_df.groupBy("fruits").sum("price")
fruits_df_pivot.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # pivot -----> is used

# COMMAND ----------

# MAGIC %md
# MAGIC pivot("Column_Name") ------->>> values in rows of column ------>>>>> become column name

# COMMAND ----------

# MAGIC %md
# MAGIC for pivoting---------> agg function must use with pivoting

# COMMAND ----------

fruits_df_pivot1 = fruits_df.groupBy("fruits").pivot("fruits").sum("price")
fruits_df_pivot1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC pivot example 2

# COMMAND ----------

fruits_df_pivot2 = fruits_df.groupBy("fruits").pivot("country").sum("price")
fruits_df_pivot2.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Different types of mode while reading a file in Dataframe

# COMMAND ----------

# Creating a list of records with both valid and bad records
data = [
    ("1", "Alice", "1000"),          # Valid record
    ("2", "Bob", None),            # Valid record
    ("3", None, "5000"),  # Bad record
    ("4", "David", "4000"),          # Valid record
    ("5", "Eve", "eight thousand"),          
]

# Define the schema
columns = ["Id", "Name", "Salary"]

# Create DataFrame
df = spark.createDataFrame(data, schema=columns)

# Show the DataFrame
df.display()

# Saving the DataFrame to a CSV file
output_path = "/FileStore/tables/sample3.csv"  # Change this path as needed
df.write.csv(output_path, header=True, mode='overwrite')


# COMMAND ----------

# MAGIC %md
# MAGIC # permissive mode

# COMMAND ----------

# MAGIC %md
# MAGIC 1.Permissive mode ( default mode)----------->>>>>give null when record is corrupt

# COMMAND ----------

# Step 1: Define the schema correctly
schema = "Id Int, Name STRING, Salary INT"

# Step 2: Read the CSV file
df = spark.read.option("header", "true") \
    .option("mode", "permissive") \
    .schema(schema) \
    .csv('dbfs:/FileStore/tables/sample3.csv')

# Display the DataFrame
df.display()

# Print the schema of the DataFrame
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # dropmalformed 

# COMMAND ----------

# MAGIC %md
# MAGIC  Dropmalformed mode-------------->>>  drop entire row>>>>> where corrupt record
# MAGIC

# COMMAND ----------

# Step 2: Read the CSV file
df = spark.read.option("header", "true") \
    .option("mode", "dropmalformed") \
    .schema(schema) \
    .csv('dbfs:/FileStore/tables/sample3.csv')

# Display the DataFrame
df.display()

# Print the schema of the DataFrame
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # failfast ---->>> give error when corrupt present

# COMMAND ----------

# MAGIC %md
# MAGIC Failfast mode—------> give error >>>>> when the corrupt record present —>>> not create df
# MAGIC

# COMMAND ----------

# Step 2: Read the CSV file
df = spark.read.option("header", "true") \
    .option("mode", "failfast") \
    .schema(schema) \
    .csv('dbfs:/FileStore/tables/sample3.csv')

# Display the DataFrame
df.display()

# Print the schema of the DataFrame
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC video 21 ,22 ,23, 24, 25 ,26, 27 skip

# COMMAND ----------

# MAGIC %md
# MAGIC # Difference between TempView and GlobalTempView

# COMMAND ----------

# Sample data
data = [
    ("2020", "Tech", "T01", 1000),
    ("2021", "Healthcare", "H01", 1500),
    ("2022", "Finance", "F01", 2000),
    ("2023", "Manufacturing", "M01", 2500),
    ("2020", "Retail", "R01", 3000),
    ("2021", "Construction", "C01", 3500),
    ("2022", "Energy", "E01", 4000),
    ("2023", "Telecom", "T02", 4500),
    ("2020", "Automotive", "A01", 5000),
    ("2021", "Education", "ED01", 5500)
]

# Define schema (column names)
columns = ["Year", "Industry_Name", "Industry_code", "Values"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.display()

# COMMAND ----------

df.write.mode("overwrite").csv("/FileStore/tables/new/samp1.csv")

# COMMAND ----------

dff =spark.read.format("csv").load('dbfs:/FileStore/tables/new/samp1.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC Every notebook -------->>>>> has separate session

# COMMAND ----------

# MAGIC %md
# MAGIC createOrReplaceTempView ------>>> session based --->>same command cant run in another notebook
# MAGIC
# MAGIC saved in default database

# COMMAND ----------

dff.createOrReplaceTempView('Test')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Test

# COMMAND ----------

dff.createTempView('Test')

# COMMAND ----------

# MAGIC %md
# MAGIC Every notebook -------->>>>> has separate session

# COMMAND ----------

# MAGIC %md
# MAGIC createOrReplaceGlobalTempView-------->>>>> not session bases---->>> same command can run in 
# MAGIC another notenook
# MAGIC
# MAGIC saved in global_temp database
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC detached and attched clustor again ------>>> not affect
# MAGIC
# MAGIC only restart clustor or shut down ----->>>> affect

# COMMAND ----------

dff.createOrReplaceGlobalTempView("Test_Global")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp. Test_Global

# COMMAND ----------

# MAGIC %md
# MAGIC video 29 skip

# COMMAND ----------

# MAGIC %md
# MAGIC # How to use InsertInto in PySpark using Databricks

# COMMAND ----------

# Sample data for the DataFrame
data = [
    ("1", "Alice"),
    ("2", "Bob"),
    ("3", "Charlie"),
    ("4", "David")
]

# Define schema (column names)
columns = ["Id", "Name"]

# Create DataFrame
df_sorce = spark.createDataFrame(data, columns)

# Show the DataFrame
df_sorce.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists testing_insert;
# MAGIC create table testing_insert
# MAGIC (
# MAGIC   Id int,
# MAGIC   Name string
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #insertInto

# COMMAND ----------

df_sorce.write.insertInto("testing_insert", overwrite=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from testing_insert

# COMMAND ----------

# MAGIC %md
# MAGIC # Difference Between Collect and Select in PySpark

# COMMAND ----------

df_sorce.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # collect

# COMMAND ----------

df_sorce.collect()

# COMMAND ----------

for i in df_sorce.collect():
    print(i.Id,i.Name)

# COMMAND ----------

df_sorce.collect()[1][1]

# COMMAND ----------

# MAGIC %md
# MAGIC # select

# COMMAND ----------

df_k = df_sorce.select("*")
df_k.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Singleline and Multiline JSON in PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC #singleline-->>json

# COMMAND ----------

# Reading single-line JSON (no need for header option)
df_singleline = spark.read.json("dbfs:/FileStore/tables/single_json/single_line.json")
df_singleline.display()


# COMMAND ----------

# MAGIC %md
# MAGIC # multiline ---->>>json

# COMMAND ----------


# Reading multi-line JSON (no need for header option)
df_multiline = spark.read.option("multiLine", "true").json("dbfs:/FileStore/tables/multi_json/multi_line.json")
df_multiline.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # What is Cache and Persist in PySpark And Spark-SQL using Databricks?

# COMMAND ----------

df = spark.read.option("header","True").option("inferSchema","true").format("csv").load("dbfs:/FileStore/tables/E/employees__1_.csv")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # cashe

# COMMAND ----------

df_cashe =spark.read.option("header","True").csv("dbfs:/FileStore/tables/E/employees__1_.csv").cache()
df_cashe.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # persist

# COMMAND ----------

df_persist = spark.read.option("header", "True").csv("dbfs:/FileStore/tables/E/employees__1_.csv")

# Step 4: Persist the DataFrame to disk only
df_persist.persist(pyspark.StorageLevel.DISK_ONLY)

# COMMAND ----------

# MAGIC %md
# MAGIC # unpersist

# COMMAND ----------

df_persist.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC In PySpark, both `cache()` and `persist()` are used for storing DataFrames in memory to speed up computations, but they have slight differences in usage:
# MAGIC
# MAGIC ### Cache
# MAGIC - **`cache()`**: This is a shorthand for `persist()` with the default storage level.
# MAGIC   - **Default Storage Level**: `MEMORY_ONLY`
# MAGIC   - **Usage**:
# MAGIC     ```python
# MAGIC     df.cache()
# MAGIC     ```
# MAGIC
# MAGIC ### Persist
# MAGIC - **`persist(storageLevel)`**: Allows you to specify the storage level explicitly.
# MAGIC   - **Common Storage Levels**:
# MAGIC     - **`MEMORY_ONLY`**: Stores DataFrame in memory (default).
# MAGIC     - **`MEMORY_ONLY_SER`**: Stores DataFrame in memory as serialized objects.
# MAGIC     - **`MEMORY_AND_DISK`**: Stores DataFrame in memory; spills to disk if it doesn't fit.
# MAGIC     - **`DISK_ONLY`**: Stores DataFrame on disk only.
# MAGIC     - **`DISK_ONLY_2`**: Two copies on disk for fault tolerance.
# MAGIC   - **Usage**:
# MAGIC     ```python
# MAGIC     from pyspark import StorageLevel
# MAGIC
# MAGIC     df.persist(StorageLevel.MEMORY_AND_DISK)  # Example of specifying storage level
# MAGIC     ```
# MAGIC
# MAGIC ### Summary
# MAGIC - **Use `cache()`** when you want the default behavior of caching in memory.
# MAGIC - **Use `persist()`** when you need to specify a different storage level based on your requirements (like memory and disk, or just disk).

# COMMAND ----------


