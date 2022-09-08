# Sales_Customer
Sales_Customer

# Import Libraries 
 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
#dbutils.fs.ls("/FileStore/tables/Bronze/")
 
#display(dbutils.fs.ls("/FileStore/tables/Bronze/"))
# Create SparkSession
# Working in the Table Sales Customer
 
spark = (
    SparkSession.builder
    .master('local')
    .appName('Project_1_Sales_Customer')
    .getOrCreate()
)
# Count of Rows
print("Reading Data Sales Customer")
df = spark.read.format("csv").option("inferSchema" , True).option("header", True ).option("sep",",").load("/FileStore/tables/Bronze/Sales_Customer.csv")
#display(Sales_Customerdf)
#print(df.count())
df.count()
Reading Data Sales Customer
Out[4]: 19820
# Read Files "csv"
 
df = spark.read.format("csv").option("inferSchema" , True).option("header", True ).option("sep",",").load("/FileStore/tables/Bronze/Sales_Customer.csv")
#display(df)
#print(df.count())
df.show(3)
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
|CustomerID|PersonID|StoreID|TerritoryID|AccountNumber|             rowguid|        ModifiedDate|
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
|         1|    NULL|    934|          1|   AW00000001|3F5AE95E-B87D-4AE...|2014-09-12 11:15:...|
|         2|    NULL|   1028|          1|   AW00000002|E552F657-A9AF-4A7...|2014-09-12 11:15:...|
|         3|    NULL|    642|          4|   AW00000003|130774B1-DB21-4EF...|2014-09-12 11:15:...|
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
only showing top 3 rows

# Check Nulls
 
df.toPandas().isna().sum()
Out[6]: CustomerID       0
PersonID         0
StoreID          0
TerritoryID      0
AccountNumber    0
rowguid          0
ModifiedDate     0
dtype: int64
df.columns
Out[7]: ['CustomerID',
 'PersonID',
 'StoreID',
 'TerritoryID',
 'AccountNumber',
 'rowguid',
 'ModifiedDate']
# Check Schema
 
df.printSchema()
root
 |-- CustomerID: integer (nullable = true)
 |-- PersonID: string (nullable = true)
 |-- StoreID: string (nullable = true)
 |-- TerritoryID: integer (nullable = true)
 |-- AccountNumber: string (nullable = true)
 |-- rowguid: string (nullable = true)
 |-- ModifiedDate: timestamp (nullable = true)

# Alter type of Metada of the Column
 
df.withColumn('StoreID', col('StoreID').cast(IntegerType())).show(3)
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
|CustomerID|PersonID|StoreID|TerritoryID|AccountNumber|             rowguid|        ModifiedDate|
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
|         1|    NULL|    934|          1|   AW00000001|3F5AE95E-B87D-4AE...|2014-09-12 11:15:...|
|         2|    NULL|   1028|          1|   AW00000002|E552F657-A9AF-4A7...|2014-09-12 11:15:...|
|         3|    NULL|    642|          4|   AW00000003|130774B1-DB21-4EF...|2014-09-12 11:15:...|
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
only showing top 3 rows

df.printSchema()
root
 |-- CustomerID: integer (nullable = true)
 |-- PersonID: string (nullable = true)
 |-- StoreID: string (nullable = true)
 |-- TerritoryID: integer (nullable = true)
 |-- AccountNumber: string (nullable = true)
 |-- rowguid: string (nullable = true)
 |-- ModifiedDate: timestamp (nullable = true)

# Datatypes Supported
 
#ByteType    ByteType()    
#ShortType    ShortType()    
#IntegerType    IntegerType()
#LongType    LongType()    
#FloatType    FloatType()    
#DoubleType    DoubleType()
#DecimalType    DecimalType()
#StringType    StringType()
#BinaryType    BinaryType()
#BooleanType    BooleanType()
#TimestampType    TimestampType()
#DateType    DateType()
 
 
#ArrayType    ArrayType(elementType, [containsNull])
#MapType    MapType(keyType, valueType, [valueContainsNull])
#StructType    StructType(fields)
#StructField    StructField(name, dataType, [nullable])
 
 
 
df.select('CustomerID PersonID StoreID TerritoryID AccountNumber ModifiedDate'.split()).show(3)
 
#df.select('CustomerID PersonID StoreID TerritoryID AccountNumber ModifiedDate'.split()).show()
+----------+--------+-------+-----------+-------------+--------------------+
|CustomerID|PersonID|StoreID|TerritoryID|AccountNumber|        ModifiedDate|
+----------+--------+-------+-----------+-------------+--------------------+
|         1|    NULL|    934|          1|   AW00000001|2014-09-12 11:15:...|
|         2|    NULL|   1028|          1|   AW00000002|2014-09-12 11:15:...|
|         3|    NULL|    642|          4|   AW00000003|2014-09-12 11:15:...|
+----------+--------+-------+-----------+-------------+--------------------+
only showing top 3 rows

# Create Global Temp View
 
df.createOrReplaceGlobalTempView('Sales_Customer.csv')
df = spark.read.format("csv").option("inferSchema" , True).option("header", True ).option("sep",",").load("/FileStore/tables/Bronze/Sales_Customer.csv")
#display(Sales_Customerdf)
#print(df.count())
df.show(3)
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
|CustomerID|PersonID|StoreID|TerritoryID|AccountNumber|             rowguid|        ModifiedDate|
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
|         1|    NULL|    934|          1|   AW00000001|3F5AE95E-B87D-4AE...|2014-09-12 11:15:...|
|         2|    NULL|   1028|          1|   AW00000002|E552F657-A9AF-4A7...|2014-09-12 11:15:...|
|         3|    NULL|    642|          4|   AW00000003|130774B1-DB21-4EF...|2014-09-12 11:15:...|
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
only showing top 3 rows

df.createOrReplaceTempView('Sales_Customer')
spark.catalog.listTables()
Out[73]: [Table(name='Sales_Customer', catalog=None, namespace=[], description=None, tableType='TEMPORARY', isTemporary=True)]
df = spark.sql ("""SELECT 
 
 CustomerID,
 PersonID,
 StoreID,
 TerritoryID,
 AccountNumber
 
 FROM Sales_Customer
 
 
 WHERE StoreID is not NULL""").show(truncate=False)
+----------+--------+-------+-----------+-------------+
|CustomerID|PersonID|StoreID|TerritoryID|AccountNumber|
+----------+--------+-------+-----------+-------------+
|1         |NULL    |934    |1          |AW00000001   |
|2         |NULL    |1028   |1          |AW00000002   |
|3         |NULL    |642    |4          |AW00000003   |
|4         |NULL    |932    |4          |AW00000004   |
|5         |NULL    |1026   |4          |AW00000005   |
|6         |NULL    |644    |4          |AW00000006   |
|7         |NULL    |930    |1          |AW00000007   |
|8         |NULL    |1024   |5          |AW00000008   |
|9         |NULL    |620    |5          |AW00000009   |
|10        |NULL    |928    |6          |AW00000010   |
|11        |NULL    |1022   |6          |AW00000011   |
|12        |NULL    |622    |6          |AW00000012   |
|13        |NULL    |434    |7          |AW00000013   |
|14        |NULL    |1020   |8          |AW00000014   |
|15        |NULL    |624    |9          |AW00000015   |
|16        |NULL    |432    |10         |AW00000016   |
|17        |NULL    |1018   |5          |AW00000017   |
|18        |NULL    |1332   |3          |AW00000018   |
|19        |NULL    |430    |1          |AW00000019   |
|20        |NULL    |1016   |1          |AW00000020   |
+----------+--------+-------+-----------+-------------+
only showing top 20 rows


