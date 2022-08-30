# Sales_Customer
Sales_Customer

# Import Libraries 
 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
#dbutils.fs.ls("/FileStore/tables/Bronze/")
 
#display(dbutils.fs.ls("/FileStore/tables/Bronze/"))
# Create SparkSession
 
spark = (
    SparkSession.builder
    .master('local')
    .appName('Project_1_Sales_Customer')
    .getOrCreate()
)
# Count of Rows
 
Sales_Customerdf = spark.read.format("csv").option("infer Schema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/Bronze/Sales_Customer.csv")
#display(Sales_Customerdf)
#print(df.count())
Sales_Customerdf.count()
Out[69]: 19820
# Read Files "csv"
 
Sales_Customerdf = spark.read.format("csv").option("infer Schema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/Bronze/Sales_Customer.csv")
#display(Sales_Customerdf)
#print(df.count())
Sales_Customerdf.show(truncate=False)
+----------+--------+-------+-----------+-------------+------------------------------------+-----------------------+
|CustomerID|PersonID|StoreID|TerritoryID|AccountNumber|rowguid                             |ModifiedDate           |
+----------+--------+-------+-----------+-------------+------------------------------------+-----------------------+
|1         |NULL    |934    |1          |AW00000001   |3F5AE95E-B87D-4AED-95B4-C3797AFCB74F|2014-09-12 11:15:07.263|
|2         |NULL    |1028   |1          |AW00000002   |E552F657-A9AF-4A7D-A645-C429D6E02491|2014-09-12 11:15:07.263|
|3         |NULL    |642    |4          |AW00000003   |130774B1-DB21-4EF3-98C8-C104BCD6ED6D|2014-09-12 11:15:07.263|
|4         |NULL    |932    |4          |AW00000004   |FF862851-1DAA-4044-BE7C-3E85583C054D|2014-09-12 11:15:07.263|
|5         |NULL    |1026   |4          |AW00000005   |83905BDC-6F5E-4F71-B162-C98DA069F38A|2014-09-12 11:15:07.263|
|6         |NULL    |644    |4          |AW00000006   |1A92DF88-BFA2-467D-BD54-FCB9E647FDD7|2014-09-12 11:15:07.263|
|7         |NULL    |930    |1          |AW00000007   |03E9273E-B193-448E-9823-FE0C44AEED78|2014-09-12 11:15:07.263|
|8         |NULL    |1024   |5          |AW00000008   |801368B1-4323-4BFA-8BEA-5B5B1E4BD4A0|2014-09-12 11:15:07.263|
|9         |NULL    |620    |5          |AW00000009   |B900BB7F-23C3-481D-80DA-C49A5BD6F772|2014-09-12 11:15:07.263|
|10        |NULL    |928    |6          |AW00000010   |CDB6698D-2FF1-4FBA-8F22-60AD1D11DABD|2014-09-12 11:15:07.263|
|11        |NULL    |1022   |6          |AW00000011   |750F3495-59C4-48A0-80E1-E37EC60E77D9|2014-09-12 11:15:07.263|
|12        |NULL    |622    |6          |AW00000012   |947BCAF1-1F32-44F3-B9C3-0011F95FBE54|2014-09-12 11:15:07.263|
|13        |NULL    |434    |7          |AW00000013   |B0FA5854-2511-439B-A7AC-50C9C460B175|2014-09-12 11:15:07.263|
|14        |NULL    |1020   |8          |AW00000014   |2F96BEDC-723D-468F-834B-B2B8AE79C849|2014-09-12 11:15:07.263|
|15        |NULL    |624    |9          |AW00000015   |0340737B-D4FA-4795-93AA-CAEB8371BCF9|2014-09-12 11:15:07.263|
|16        |NULL    |432    |10         |AW00000016   |C9381589-D31C-4EFE-8978-8D3449EB1F0F|2014-09-12 11:15:07.263|
|17        |NULL    |1018   |5          |AW00000017   |34DB417F-1E0B-4408-9FF6-987E59D0D73C|2014-09-12 11:15:07.263|
|18        |NULL    |1332   |3          |AW00000018   |C04D6B4D-94C6-4C5C-A44C-B449C0AC1B45|2014-09-12 11:15:07.263|
|19        |NULL    |430    |1          |AW00000019   |69AE5D43-31BE-4B76-BFBB-5A23C4788BBC|2014-09-12 11:15:07.263|
|20        |NULL    |1016   |1          |AW00000020   |E010C10A-F1C3-4BBA-81CA-A7E083350400|2014-09-12 11:15:07.263|
+----------+--------+-------+-----------+-------------+------------------------------------+-----------------------+
only showing top 20 rows

# Check Nulls
 
Sales_Customerdf.toPandas().isna().sum()
Out[49]: CustomerID       0
PersonID         0
StoreID          0
TerritoryID      0
AccountNumber    0
rowguid          0
ModifiedDate     0
dtype: int64
Sales_Customerdf.columns
Out[56]: ['CustomerID',
 'PersonID',
 'StoreID',
 'TerritoryID',
 'AccountNumber',
 'rowguid',
 'ModifiedDate']
# Check Schema
Sales_Customerdf.printSchema()
root
 |-- CustomerID: string (nullable = true)
 |-- PersonID: string (nullable = true)
 |-- StoreID: string (nullable = true)
 |-- TerritoryID: string (nullable = true)
 |-- AccountNumber: string (nullable = true)
 |-- rowguid: string (nullable = true)
 |-- ModifiedDate: string (nullable = true)

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
 
#Sales_Customerdf.withColumn('ModifiedDate', col('ModifiedDate').cast(IntegerType())).show(truncate=False)
 
Sales_Customerdf.show(3)
