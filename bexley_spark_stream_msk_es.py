#PySpark Code to consume data stream from kafka cluste, transform and persist to ES.
# Pre-requisites : kafka and mysql jar in spark jar directory
#Infrastructure : Amazon MSK, Amazon Opensearch, Spark cluster, 
# V1 : Sept 16th 2022

from pyspark import SparkConf, SparkContext, sql
from pyspark.sql.functions import from_utc_timestamp
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, lit, concat_ws, substring, when
from ast import literal_eval
from pyspark.sql.types import StructType,StructField,  StringType, IntegerType,BooleanType, DateType, DoubleType, MapType, TimestampType, ArrayType
import boto3
import json
import sys
from botocore.exceptions import ClientError


try:

    region_name = "eu-west-2"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    rds_response = client.get_secret_value(
            SecretId='rds-secret'
        )

    es_response = client.get_secret_value(
            SecretId='es_secret'
        )
    rds_secrets = json.loads(rds_response['SecretString'])
    es_secrets = json.loads(es_response['SecretString'])

except ClientError as e:
    raise e
    sys.exit(1)

#declare variables to hold kafk, ES and RDS details

rds_user = rds_secrets['username']
rds_password = rds_secrets['password']
rds_endpoint = "jdbc:mysql://ecomm-db-replica-01.xxxxxxxx.eu-west-2.rds.amazonaws.com:3306/ecomm_salesdb"


kafka_topic_name = "elk-stream-01"
kafka_bootstrap_servers = "b-2.shoppingmsk01.xxxxxx.xx.kafka.eu-west-2.amazonaws.com:9092,b-1.shoppingmsk01.xxxxxx.xx.kafka.eu-west-2.amazonaws.com:9092"

es_domain = "https://search-bexley-shopping-xxxxxxxxxxxxxxxxx.eu-west-2.es.amazonaws.com"
es_index = "bexley_orders"
es_user = es_secrets['username']
es_password = es_secrets['password']

spark_master = "spark_master_endpoint"

spark = SparkSession \
        .builder \
        .appName("App Bexley") \
        .master(spark_master) \
        .getOrCreate()  \

spark.sparkContext.setLogLevel("ERROR")

#create data frame from  NUS data sets

# Construct a streaming DataFrame that reads from topic
# default for startingOffsets is "latest", but "earliest" allows rewind for missed alerts 

stream_df_00 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "earliest") \
    .load() 

# CREATE schema for value dataframe
#basket column is an array of structs

stream_schema = StructType([ \
      StructField("order_id",IntegerType(),True), \
      StructField("order_total",DoubleType(),True), \
      StructField("ship_to_city_id" ,IntegerType(),True) ,\
      StructField("freight" ,DoubleType(),True), \
      StructField("customer_id" ,IntegerType(),True), \
      StructField("ship_method_id",IntegerType(),True), \
      StructField("order_number" ,StringType(),True), \
      StructField("discount_applied" ,DoubleType(),True), \
      StructField("order_date" ,StringType(),True), \
      StructField("order_basket", ArrayType(
        StructType(
           [
            StructField("order_qty", IntegerType()),
            StructField("product_id",IntegerType()),
            StructField("is_discounted",BooleanType())
            ]
        ))
      )
])

print("output schema of the kafka stream")
stream_df_00.printSchema()

print("cast the binary column containing the stream data to the string type")

stream_df_01 = stream_df_00.selectExpr("CAST(value as STRING)").toDF("value")
stream_df_01.printSchema()

print("apply a defined schema to the datframe")
stream_df_02 = stream_df_01.select(from_json(col("value"),stream_schema).alias("temptable"))
stream_df_02.printSchema()

print("Select all columns from the data frame")
stream_df_02_b = stream_df_02.select("temptable.*", explode(col("temptable.order_basket")).alias("basket_exp"))

stream_df_02_b.printSchema()

#lets pick out a mapping of some order details with the line item details on separate rows

stream_df_02_c = stream_df_02_b.select("*", "basket_exp.product_id","basket_exp.order_qty")

print("exploded schema of the data frame")
stream_df_02_c.printSchema()

print("Select specific colums from the data frame, transforming alongside")
# use mid string to determine merchant product

stream_df_03 = stream_df_01.select(from_json(col("value"),stream_schema).alias("temptable")) \
                     .select(
                     col("temptable.order_number"),   
                     (lit(col("temptable.order_total")) - lit(col("temptable.discount_applied")/100)*lit(col("temptable.order_total"))).alias("discounted_total"), \
		             concat_ws('-', col("temptable.order_number"),substring(col("temptable.order_date"),1,10)).alias("data_key"),\
                     col("temptable.ship_to_city_id"), \
                     col("temptable.order_date"), \
		     (when( substring(col("temptable.order_number"),6,1)=="3","Bexley").otherwise("Merchant")).alias("fufilment_type")
             )


print("stream_df_03")
stream_df_03.printSchema()


print("stream_df_04")

# create data frame for cities

ship_cities_df = spark.read.format("jdbc").options(
url=rds_endpoint,
driver='com.mysql.cj.jdbc.Driver',
user=rds_user,
password=rds_password).option("query", "SELECT city_id, city FROM vw_uk_cities").load()  

print ("preview the schema of shipping destinations")

ship_cities_df.printSchema()

print ("preview first 10 shipping destinations")

print(ship_cities_df.sample(0.08).collect())

#finally lets enrich the orders stream with the shipping destinations data frame, in a stream-static join

stream_df_04 = stream_df_03.join(ship_cities_df,stream_df_03.ship_to_city_id == ship_cities_df.city_id,"left").drop(ship_cities_df.city_id)


# write to ES
# define function to write micro batches

#specify key for index dont default to _id 
def foreach_batch(stream_df_04, epoch_id):
#     Transform and write the dataframe
	 stream_df_04.write \
	.mode("append") \
	.format("es") \
	.option("es.nodes", es_domain) \
	.option("es.port", "443") \
	.option("es.net.http.auth.user", es_user) \
	.option("es.net.http.auth.pass", es_password) \
	.option("es.nodes.wan.only","true") \
    .option("es.mapping.id", "data_key")   \
    .option("checkpointLocation", "/logs/kafka/bexley-logs") \
	.option("es.resource",es_index) \
	.save()

print("status :", stream_df_04.isStreaming)

stream_df_05 = stream_df_04.writeStream.foreachBatch(foreach_batch).queryName("bexley-stream").start()

stream_df_05.awaitTermination()
