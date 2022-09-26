#Use spark-submit to the the script

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

app_name = "Retail"
master = "local"

spark = SparkSession.builder.master(master).appName(app_name).enableHiveSupport().getOrCreate()

#Read databases present in HIVE.
df = spark.sql("show databases")
df.show()

#Read from HDFS
#Create Schema First
region_schema = StructType() \
        .add(StructField("id",IntegerType(),True)) \
        .add(StructField("name",StringType(),True)) \
        .add(StructField("comment",StringType(),True))
#Read csv from HDFS Location
df = spark.read.csv("hdfs://localhost:9000/user/ak/retail/region.csv", schema=region_schema, sep='\t', header=True)
df.show(3)
