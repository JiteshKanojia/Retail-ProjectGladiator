import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
app_name = "hive_pyspark"
master = "local"

spark = SparkSession.builder.master(master).appName(app_name).enableHiveSupport().getOrCreate()

df = spark.sql("show databases")
df.show()
