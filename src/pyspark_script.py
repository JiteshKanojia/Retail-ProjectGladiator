#NOTE: Use this command to submit spark application
#    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark_script.py 

import pyspark
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType,StructField,DateType,IntegerType,FloatType,StringType,DoubleType

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
nations_df = spark.sql("select * from nations")
nations_df.show(5)
nations_df.printSchema()
print("#####"*20)

##Read from mySQL
#define schema to read

lineitems_schema = StructType() \
        .add(StructField("orderId", IntegerType(), True)) \
        .add(StructField("partId", IntegerType(), True)) \
        .add(StructField("supplyId", IntegerType(), True)) \
        .add(StructField("lineNumber", IntegerType(), True)) \
        .add(StructField("quantity", IntegerType(), True)) \
        .add(StructField("extendedPrice", DoubleType(), True)) \
        .add(StructField("discount", DoubleType(), True)) \
        .add(StructField("tax", DoubleType(), True)) \
        .add(StructField("returnFlag", StringType(), True)) \
        .add(StructField("lineStatus", StringType(), True)) \
        .add(StructField("shipDate", DateType(), True)) \
        .add(StructField("commitDate", DateType(), True)) \
        .add(StructField("receiptDate", DateType(), True)) \
        .add(StructField("shipInstruct", StringType(), True)) \
        .add(StructField("shipMode", StringType(), True)) \
        .add(StructField("comment", StringType(), True)) 
#read data from mysql table
lineitems = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/retail").option("dbtable","lineitems").option("user","ak").option("password","ak").option("schema",lineitems_schema).load()

lineitems.printSchema()
lineitems.show(5)

#Transformations for business requirements
#Total extended price
total_extended_price = lineitems.groupBy('returnFlag', 'lineStatus').agg(round(sum('extendedPrice'), 2).alias('ExtendedPrice'), sum('quantity').alias('Quantity')).orderBy('returnFlag', 'lineStatus')

total_extended_price.show(5)


#Total Discounted Price
lineitems = lineitems.withColumn('discountedExtendedPrice', col('extendedPrice') - (col('extendedPrice') * col('discount')) )

total_discounted_extended_price = lineitems.groupBy('returnFlag', 'lineStatus').agg(round(sum('discountedExtendedPrice'), 2).alias('DiscExtPrice'), sum('quantity').alias('Quantity')).orderBy('returnFlag', 'lineStatus')

print("##"*25)
total_discounted_extended_price.show()

# Discounted Extended Price + Tax
lineitems = lineitems.withColumn('discWTax',  col('discountedExtendedPrice') + (col('discountedExtendedPrice') * col('tax')) )
total_discounted_extended_taxed = lineitems.groupBy('returnFlag', 'lineStatus').agg(round(sum('discWTax'), 2).alias('DiscExtWithTax'), sum('quantity').alias('Quantity') ).orderBy('returnFlag', 'lineStatus')

print("###"*25)
total_discounted_extended_taxed.show()

# Final Transformed Data
finalData = lineitems.groupBy('returnFlag', 'lineStatus').agg( sum('quantity').alias('Quantity'), round(sum('extendedPrice'), 2).alias('ExtendedPrice'), round(avg('extendedPrice'), 2).alias('AVG_ExtendedPrice'), round(sum('discountedExtendedPrice'), 2).alias('DiscExtPrice'), round(avg('discountedExtendedPrice'), 2).alias('AVG_DiscExtPrice'), round(sum('discWTax'), 2).alias('DiscExtWithTax'), round(avg('discWTax'), 2).alias('AVG_discWTax') ).orderBy('returnFlag', 'lineStatus')

print("###"*25)
finalData.show()

#Save Transformations to CSV format
total_extended_price.write.format("csv").mode("overwrite").option("header","true").save("../outputData/csv/TotalExtendedPrice")
total_discounted_extended_price.write.format("csv").mode("overwrite").option("header","true").save("../outputData/csv/TotalDiscountedExtendedPrice")  
total_discounted_extended_taxed.write.format("csv").mode("overwrite").option("header","true").save("../outputData/csv/TotalDiscountedExtendedTaxedPrice")
finalData.write.format("csv").mode("overwrite").option("header","true").save('../outputData/csv/FinalTransfomedData')

#Save Transformations to parquet format
total_extended_price.write.format("parquet").mode("overwrite").option("header","true").save("../outputData/parquet/TotalExtendedPrice")
total_discounted_extended_price.write.format("parquet").mode("overwrite").option("header","true").save("../outputData/parquet/TotalDiscountedExtendedPrice")
total_discounted_extended_taxed.write.format("parquet").mode("overwrite").option("header","true").save("../outputData/parquet/TotalDiscountedExtendedTaxedPrice")
finalData.write.format("parquet").mode("overwrite").option("header","true").save('../outputData/parquet/FinalTransfomedData')



#Read data from Kafka Instance and print to console


stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "json_topic") \
    .option("startingOffsets","earliest") \
    .load()

stream_df.printSchema()
stream_df = stream_df.selectExpr("key","CAST(value AS STRING)","timestamp")
query = stream_df.writeStream.trigger(processingTime = "10 seconds").outputMode("append").format("console").start()
query.awaitTermination()


