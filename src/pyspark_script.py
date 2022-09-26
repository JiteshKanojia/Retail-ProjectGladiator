#Use spark-submit to the the script

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType,StructField,DateType,IntegerType,FloatType,StringType

app_name = "Retail"
master = "local"
warehouse_location = "retail"

spark = SparkSession.builder.master(master).appName(app_name).config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()

#Read databases present in HIVE.
hive_df = spark.sql("show databases")
hive_df.show()

#Read from HDFS
#Create Schema First
order_schema = StructType() \
        .add(StructField("orderId",IntegerType(),True)) \
        .add(StructField("customerId",IntegerType(),True)) \
        .add(StructField("orderStatus",StringType(),True)) \
        .add(StructField("totalPrice",FloatType(),True)) \
        .add(StructField("orderDate",DateType(),True)) \
        .add(StructField("orderPriority",StringType(),True)) \
        .add(StructField("clerk",StringType(),True)) \
        .add(StructField("shipPriority",IntegerType(),True)) \
        .add(StructField("comment",StringType(),True))


#Read csv from HDFS Location
orders_df = spark.read.csv("hdfs://localhost:9000/user/ak/retail/orders.csv", schema=order_schema, sep='\t', header=True)
orders_df.show(3)
print("#####"*20)

#hiveQL load data from nations table to df
spark.sql("use retail")
hive_df = spark.sql("select * from nations")
hive_df.show(5)
hive_df.printSchema()
print("#####"*20)

