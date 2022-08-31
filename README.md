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
Sales_Customerdf = spark.read.format("csv").option("inferSchema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/Bronze/Sales_Customer.csv")
#display(Sales_Customerdf)
#print(df.count())
Sales_Customerdf.count()
Reading Data Sales Customer
Out[71]: 19820
# Read Files "csv"
 
Sales_Customerdf = spark.read.format("csv").option("inferSchema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/Bronze/Sales_Customer.csv")
#display(Sales_Customerdf)
#print(df.count())
Sales_Customerdf.show(10)
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
|CustomerID|PersonID|StoreID|TerritoryID|AccountNumber|             rowguid|        ModifiedDate|
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
|         1|    NULL|    934|          1|   AW00000001|3F5AE95E-B87D-4AE...|2014-09-12 11:15:...|
|         2|    NULL|   1028|          1|   AW00000002|E552F657-A9AF-4A7...|2014-09-12 11:15:...|
|         3|    NULL|    642|          4|   AW00000003|130774B1-DB21-4EF...|2014-09-12 11:15:...|
|         4|    NULL|    932|          4|   AW00000004|FF862851-1DAA-404...|2014-09-12 11:15:...|
|         5|    NULL|   1026|          4|   AW00000005|83905BDC-6F5E-4F7...|2014-09-12 11:15:...|
|         6|    NULL|    644|          4|   AW00000006|1A92DF88-BFA2-467...|2014-09-12 11:15:...|
|         7|    NULL|    930|          1|   AW00000007|03E9273E-B193-448...|2014-09-12 11:15:...|
|         8|    NULL|   1024|          5|   AW00000008|801368B1-4323-4BF...|2014-09-12 11:15:...|
|         9|    NULL|    620|          5|   AW00000009|B900BB7F-23C3-481...|2014-09-12 11:15:...|
|        10|    NULL|    928|          6|   AW00000010|CDB6698D-2FF1-4FB...|2014-09-12 11:15:...|
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
only showing top 10 rows

# Check Nulls
 
Sales_Customerdf.toPandas().isna().sum()
Out[73]: CustomerID       0
PersonID         0
StoreID          0
TerritoryID      0
AccountNumber    0
rowguid          0
ModifiedDate     0
dtype: int64
Sales_Customerdf.columns
Out[74]: ['CustomerID',
 'PersonID',
 'StoreID',
 'TerritoryID',
 'AccountNumber',
 'rowguid',
 'ModifiedDate']
# Check Schema
 
Sales_Customerdf.printSchema()
root
 |-- CustomerID: integer (nullable = true)
 |-- PersonID: string (nullable = true)
 |-- StoreID: string (nullable = true)
 |-- TerritoryID: integer (nullable = true)
 |-- AccountNumber: string (nullable = true)
 |-- rowguid: string (nullable = true)
 |-- ModifiedDate: timestamp (nullable = true)

# Alter type of Metada of the Column
 
Sales_Customerdf.withColumn('StoreID', col('StoreID').cast(IntegerType())).show(10)
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
|CustomerID|PersonID|StoreID|TerritoryID|AccountNumber|             rowguid|        ModifiedDate|
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
|         1|    NULL|    934|          1|   AW00000001|3F5AE95E-B87D-4AE...|2014-09-12 11:15:...|
|         2|    NULL|   1028|          1|   AW00000002|E552F657-A9AF-4A7...|2014-09-12 11:15:...|
|         3|    NULL|    642|          4|   AW00000003|130774B1-DB21-4EF...|2014-09-12 11:15:...|
|         4|    NULL|    932|          4|   AW00000004|FF862851-1DAA-404...|2014-09-12 11:15:...|
|         5|    NULL|   1026|          4|   AW00000005|83905BDC-6F5E-4F7...|2014-09-12 11:15:...|
|         6|    NULL|    644|          4|   AW00000006|1A92DF88-BFA2-467...|2014-09-12 11:15:...|
|         7|    NULL|    930|          1|   AW00000007|03E9273E-B193-448...|2014-09-12 11:15:...|
|         8|    NULL|   1024|          5|   AW00000008|801368B1-4323-4BF...|2014-09-12 11:15:...|
|         9|    NULL|    620|          5|   AW00000009|B900BB7F-23C3-481...|2014-09-12 11:15:...|
|        10|    NULL|    928|          6|   AW00000010|CDB6698D-2FF1-4FB...|2014-09-12 11:15:...|
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
only showing top 10 rows

Sales_Customerdf.printSchema()
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
 
 
# Alter type of Metada of the Column
 
Sales_Customerdf.select('CustomerID PersonID StoreID TerritoryID AccountNumber ModifiedDate'.split()).show(10)
 
 
#Sales_Customerdf.select('CustomerID PersonID StoreID TerritoryID AccountNumber ModifiedDate'.split()).show()
+----------+--------+-------+-----------+-------------+--------------------+
|CustomerID|PersonID|StoreID|TerritoryID|AccountNumber|        ModifiedDate|
+----------+--------+-------+-----------+-------------+--------------------+
|         1|    NULL|    934|          1|   AW00000001|2014-09-12 11:15:...|
|         2|    NULL|   1028|          1|   AW00000002|2014-09-12 11:15:...|
|         3|    NULL|    642|          4|   AW00000003|2014-09-12 11:15:...|
|         4|    NULL|    932|          4|   AW00000004|2014-09-12 11:15:...|
|         5|    NULL|   1026|          4|   AW00000005|2014-09-12 11:15:...|
|         6|    NULL|    644|          4|   AW00000006|2014-09-12 11:15:...|
|         7|    NULL|    930|          1|   AW00000007|2014-09-12 11:15:...|
|         8|    NULL|   1024|          5|   AW00000008|2014-09-12 11:15:...|
|         9|    NULL|    620|          5|   AW00000009|2014-09-12 11:15:...|
|        10|    NULL|    928|          6|   AW00000010|2014-09-12 11:15:...|
+----------+--------+-------+-----------+-------------+--------------------+
only showing top 10 rows

# Create Global Temp View
 
Sales_Customerdf.createOrReplaceGlobalTempView('Sales_Customer.csv')
