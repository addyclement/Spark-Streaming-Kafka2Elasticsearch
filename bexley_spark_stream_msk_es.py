# PySpark Code to consume data stream from kafka cluster, transform and persist to ES.
# Pre-requisites : kafka and mysql jar in spark jar directory
# Infrastructure : Amazon MSK, Amazon Opensearch, Spark cluster on EKS, 
# Author : Addy Clement
# V1 Initial Deployment  : Aug 11th, 2020
# V2 Major Revision : Sept 16th 2022
# V3 Minor Revision : Nov 19th 2022
# V4 Major Revision : Nov 17th 2023


from pyspark import SparkConf, SparkContext, sql
from pyspark.sql.functions import from_utc_timestamp
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, lit, concat_ws, substring, when, window,count
from ast import literal_eval
from pyspark.sql.types import StructType,StructField,  StringType, IntegerType,BooleanType, DateType, DoubleType, MapType, TimestampType, ArrayType
import boto3
import json
import sys
from botocore.exceptions import ClientError
from bexley_load_auth_from_secrets_manager_v01 import get_secret_from_sm
from decouple import config
import io
import logging
import ecs_logging

# define the main function
global es_domain, es_index, es_user, es_password, es_port

# arn:aws:s3:::bexley-checkpointdir
# s3://bexley-checkpointdir/spark/
#***************# Set up Logging ***************#

logger = logging.getLogger(__name__)
# logger = logging.getLogger("app")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(ecs_logging.StdlibFormatter())
logger.addHandler(handler)


#***************# Load details for elasticsearch ***************#

es_secret_id = config('bexley_es_secret_id')

es_secrets = get_secret_from_sm(es_secret_id)

es_index = es_secrets['index']
es_domain = es_secrets['endpoint']
es_user = es_secrets['username']
es_password = es_secrets['password']
es_port = es_secrets['port']

# use kwargs here

def init_spark(spark_master):
  spark = SparkSession \
            .builder \
            .master(spark_master) \
            .appName("Bexley Streaming Service") \
            .getOrCreate()  
  
  return spark

def main():

    #*****# Load Secret ids from environment variable #*****#

    mysql_secret_id = config('bexley_mysql_secret_id')
    kafka_secret_id = config('bexley_kafka_secret_id')
    

    #*****# Decrypt auth secrets stored in Secrets Manager *******#

    rds_secrets = get_secret_from_sm(mysql_secret_id)
    kafka_secrets = get_secret_from_sm(kafka_secret_id)
    

    #***************# Load details for RDS MySQL ***************#

    rds_user = rds_secrets['username']
    rds_password = rds_secrets['password']
    rds_port=rds_secrets['port']
    rds_database=rds_secrets['dbname']
    rds_endpoint = rds_secrets['host']
    rds_mysql_url = "jdbc:mysql://{host}:{port}/{db}?autoReconnect=true&useSSL=false".format(host=rds_endpoint,
                                                                                             port=rds_port,
                                                                                             db=rds_database
                                                                                            )
    
    shipping_destination_view = "SELECT city_id, city FROM vw_uk_cities"

    #***************# Load details for kafka ***************#

    kafka_topic_name = kafka_secrets['bexkley_topic']
    kafka_bootstrap_servers = kafka_secrets['bootstrap_servers']
    kafka_sasl_plain_username = kafka_secrets['sasl_plain_username']
    kafka_sasl_plain_password = kafka_secrets['sasl_plain_password']
    kafka_sasl_mechanism = kafka_secrets['sasl_mechanism']
    kafka_security_protocol = kafka_secrets['security_protocol']
    kafka_max_offset_per_trigger = 100
    kafka_starting_offset = "latest"   # "latest"


    spark_master_endpoint = config('bexley_spark_master_endpoint')

    # declare a global spark session
    #             .master(spark_master) \
    global spark
    
    #Initialise the spark session
    spark = init_spark(spark_master=spark_master_endpoint)

    # spark.sparkContext.setLogLevel("ERROR")

    sc = spark.sparkContext

    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    

    spark.sparkContext.setLogLevel("ERROR")
    # spark.conf.set('spark.sql.shuffle.partitions','10000')
    spark.conf.set("spark.hadoop.fs.s3a.fast.upload",True)

    # *************************************
    # Extract Phase
    # *************************************

    # consume data from kafka stream

    kafka_raw_extract_df=extract_json_from_kafka_stream(bootstrap_server=kafka_bootstrap_servers,
                                    topic=kafka_topic_name,
                                    sasl_username=kafka_sasl_plain_username,
                                    sasl_password=kafka_sasl_plain_password, 
                                    security_protocol=kafka_security_protocol,
                                    sasl_mechanism=kafka_sasl_mechanism,               
                                    st_offset=kafka_starting_offset                  
                                   )
    #  max_offset=kafka_max_offset_per_trigger
    
    # read shipping destinations from RDS MySQL

    shipping_destinations_df = extract_ship_cities_from_mysql(db_url=rds_mysql_url,
                                                              db_user=rds_user,
                                                              db_password=rds_password,
                                                              db_query=shipping_destination_view)

    # *************************************
    # Transform Phase
    # *************************************
    # transform the raw data frame
    transfomed_json_df = transform_raw_dataframe(raw_dataframe=kafka_raw_extract_df)

    # transform the transformed json df
    curated_stream_df = transform_json_message(json_df=transfomed_json_df) 

    # Generate Tumbling window Stats
    # tumbling_window_df = transform_tumbling_window(df=curated_stream_df,ts_col="order_date")


    #stream enrichment - stream - static join
    # CHANGE 3 COMMENT
    enriched_stream_df = transform_stream_static_join(curated_stream_df,shipping_destinations_df)
    

    # *************************************
    # Load Phase
    # *************************************

    print("writing enriched data set to console ...")
    
    # CHANGE 1 :  original value = enriched_stream_df
    #tumbling_window_df.printSchema()
    # write_enriched_df_to_console(stream=curated_stream_df)
    # write_enriched_df_to_console(stream=tumbling_window_df)

    print("writing micro batch enriched stream to ES sink")

    # commented out for console testing
    write_enriched_df_to_sink(df=enriched_stream_df,appname="bexley-shopping")


    # Functions begin here

    # *************************************
    # Functions Definition
    # *************************************


def extract_json_from_kafka_stream(bootstrap_server, topic, sasl_username,sasl_password,sasl_mechanism,security_protocol, st_offset):

    # Construct a streaming DataFrame that reads from topic
    # default for startingOffsets is "latest", but "earliest" allows rewind for missed alerts 

    # "group.id": group_id
    # https://canonical.com/data/docs/spark/k8s/h-spark-streaming
    # https://docs.aws.amazon.com/glue/latest/webapi/API_KafkaStreamingSourceOptions.html


    kafka_options = {
                "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="USERNAME" password="PASSWORD";',
                "kafka.sasl.mechanism": sasl_mechanism,
                "kafka.security.protocol" : security_protocol,
                "kafka.sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username={username} password={password};".format(username=sasl_username,password=sasl_password) ,
                "includeHeaders": "true",
                "kafka.bootstrap.servers": bootstrap_server,
                "subscribe": topic,
                "startingOffsets": st_offset
                
            }
    # "maxOffsetsPerTrigger":max_offset

    try :
        stream_df_00 = spark.readStream.format("kafka").options(**kafka_options).load()
        logger.info("Kafka Stream Dataframe Created",extra={"http.request.body.content": "Kafka Stream Dataframe Created"})
        return stream_df_00
    except Exception as ex :
        # print("Full Error response : ", ex)
        logger.error("Error reading Kafka Stream",extra={"http.request.body.content": ex})
        sys.exit(1)

# function to extract shipping destinations from RDS MySQL DB

def extract_ship_cities_from_mysql_01(host, username, password,query):
        # create data frame for cities

    try:

        ship_cities_df = spark.read.format("jdbc").options(
        url=host,
        driver='com.mysql.jdbc.Driver',
        user=username,
        password=password).option("query", query).load()  

        print ("preview the schema of shipping destinations")

        ship_cities_df.printSchema()

        print ("preview a few shipping destinations")

        print(ship_cities_df.sample(0.08).collect())

        logger.info("Shipping Locations Dataframe Created",extra={"http.request.body.content": "Shipping Locations Dataframe Created"})

        # return a shipping destination dataframe
        return ship_cities_df

    except Exception as ex:
        
        logger.error("Error reading shipping destinations",extra={"http.request.body.content": ex})
        sys.exit(1)

def extract_ship_cities_from_mysql(db_url,db_query,db_user,db_password,db_driver='com.mysql.jdbc.Driver') :
    
    try:
        df_all_shipping_locations = spark.read.format("jdbc").options(
        url=db_url,
        driver=db_driver,
        user=db_user,
        password=db_password).option("query", db_query).load()

        custom_log = "Data fetched successfully from MySQL S6 Database"
        print(custom_log)
        logger.info(custom_log, extra={"http.request.body.content": custom_log})

        print ("preview the schema of shipping destinations")

        df_all_shipping_locations.printSchema()

        print ("preview a few shipping destinations")

        print(df_all_shipping_locations.sample(0.08).collect())

        # return a shipping destination dataframe
        logger.info("Shipping Locations Dataframe Created",extra={"http.request.body.content": "Shipping Locations Dataframe Created"})


        return  df_all_shipping_locations
            
    except Exception as ex:
        
        logger.error("Error reading shipping destinations",extra={"http.request.body.content": ex})
        #exit on error
        sys.exit(1)

def transform_raw_dataframe(raw_dataframe):
    # Construct a schema for value dataframe
    # basket column is an array of structs

    print("output schema of the kafka stream")
    raw_dataframe.printSchema()

    print("cast the binary column containing the stream data to the string type")

    try:

        stream_df_01 = raw_dataframe.selectExpr("CAST(value as STRING)").toDF("value")
        stream_df_01.printSchema()

        # change ship method to String
        
        stream_schema = StructType([ \
            StructField("order_id",IntegerType(),True), \
            StructField("order_total",DoubleType(),True), \
            StructField("ship_to_city_id" ,IntegerType(),True) ,\
            StructField("freight" ,DoubleType(),True), \
            StructField("customer_id" ,IntegerType(),True), \
            StructField("ship_method",StringType(),True),   
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

        print("apply a defined schema to the dataframe")
        stream_df_02 = stream_df_01.select(from_json(col("value"),stream_schema).alias("temptable"))
        stream_df_02.printSchema()

        stream_df_02_a = stream_df_02.select("temptable.*")
        print("Show schema of original json message")
        stream_df_02_a.printSchema()

        logger.info("Initial Raw Dataframe Transformed",extra={"http.request.body.content": "Initial Raw Dataframe Transformed"})


        return stream_df_02_a 
    
    except Exception as ex:
        
        logger.error("Problem Transforming Initial/Raw Dataframe",extra={"http.request.body.content": ex})
        #exit on error
        sys.exit(1)


def transform_json_message(json_df):
    # Construct a schema for value dataframe
    # basket column is an array of structs


    try:

        print("Select all columns from the data frame, and explode the order basket")

        stream_df_02_b = json_df.select("*", explode(col("order_basket")).alias("basket_exp"))

        stream_df_02_b.printSchema()

        #lets pick out a mapping of some order details with the line item details on separate rows

        stream_df_02_c = stream_df_02_b.select("*", "basket_exp.product_id","basket_exp.order_qty")

        stream_order_products = stream_df_02_b.select("customer_id","order_number", "basket_exp.product_id","basket_exp.order_qty")

        print("show schema for products sold ...")
        stream_order_products.printSchema()

        print("exploded schema of the data frame")
        stream_df_02_c.printSchema()

        print("Select specific colums from the data frame, transforming alongside")
        # use mid string to determine merchant product

        # extract ship method to the data frame
        # we could pretty much do the expansion in one go
        # reading the value from the origonal kafka data
        # apply a schema 
        # and select the columns we need
        # we have intentional simplified the stages,to allow us run unit tests with raw json data 

        """

        stream_df_03 = json_df.select(from_json(col("value"),stream_schema).alias("temptable")) \
                            .select(
                            col("temptable.order_number"),   
                            (lit(col("temptable.order_total")) - lit(col("temptable.discount_applied")/100)*lit(col("temptable.order_total"))).alias("discounted_total"), \
                            concat_ws('-', col("temptable.order_number"),substring(col("temptable.order_date"),1,10)).alias("data_key"),\
                            col("temptable.ship_to_city_id"), \
                            col("temptable.order_date"), \
                            col("temptable.ship_method"), \
                    (when( substring(col("temptable.order_number"),6,1)=="3","Bexley").otherwise("Merchant")).alias("fufilment_type")
                    )
        """

        stream_df_03 = json_df.select(
                            col("order_number"),   
                            (lit(col("order_total")) - lit(col("discount_applied")/100)*lit(col("order_total"))).alias("discounted_total"), \
                            concat_ws('-', col("order_number"),substring(col("order_date"),1,10)).alias("data_key"),\
                            col("ship_to_city_id"), \
                            col("order_date"), \
                            col("ship_method"), \
                    (when( substring(col("order_number"),6,1)=="3","Bexley").otherwise("Merchant")).alias("fufilment_type")
                    )

        print("printing out the schema for the transformed kafka feed")
        stream_df_03.printSchema()

        print("Initial Transformation of Raw Kafka Stream Successful ...")

        logger.info("JSON Dataframe Transformed",extra={"http.request.body.content": "JSON Dataframe Transformed"})


        return stream_df_03 
    # return stream_order_products 
    except Exception as ex:
        
        logger.error("Problem Transforming JSON Dataframe",extra={"http.request.body.content": ex})
        #exit on error
        sys.exit(1)


    # merge the streams from kafka and mysql
def transform_stream_static_join(kafka_df, mysql_df):

    #finally lets enrich the orders stream with the shipping destinations data frame, in a stream-static join
    try:

        stream_df_04 = kafka_df.join(mysql_df,kafka_df.ship_to_city_id == mysql_df.city_id,"left").drop(mysql_df.city_id)
        
        print("Stream static join enrichment - Succeeded")
        logger.info("Stream static join enrichment - Succeeded",extra={"http.request.body.content": "Stream static join enrichment - Succeeded"})

        return stream_df_04
    
    except Exception as ex:
        print("Problem with stream static join")
        print("Full Error response : ", ex)
        
        logger.error("Stream static join enrichment - Failed",extra={"http.request.body.content": ex})
        #exit on error
        sys.exit(1)

def transform_tumbling_window(df,ts_col):

    #finally lets enrich the orders stream with the shipping destinations data frame, in a stream-static join
    try:

        df_series = df.withColumn(ts_col,col(ts_col).cast(TimestampType()))
        #tumbling_window = df_series.withWatermark("order_date", "2 minutes").groupBy(window("order_date", "2 minutes"),"fufilment_type").count().alias("TotalOrders").orderBy("window")
        tumbling_window = df_series.withWatermark("order_date", "2 minutes").groupBy(window("order_date", "2 minutes"),"fufilment_type").agg(count("ship_method").alias("total_orders")).orderBy("window")
        tumbling_window.printSchema()
        logger.info("2 minute order stats",extra={"http.request.body.content": "2 minute order stats"})

        return tumbling_window
    
    except Exception as ex:
        print(ex)
        # logger.error("Problem Genrating Tumbling Window",extra={"http.request.body.content": ex})
        #exit on error
        sys.exit(1)

# s3://bexley-checkpointdir/spark/
# /Users/addyclement/spark/checkpointdir
# option("checkpointLocation", "/Users/addyclement/spark/checkpointdir")
# .queryName("bexley-live-stream") \
        
def write_enriched_df_to_console(stream):
    
    try:
        # change from append to complete, append required for raw value, complete for aggregation
        stream.writeStream.format("console") \
        .outputMode("complete") \
        .option("truncate", False) \
        .start().awaitTermination()  

    except Exception as ex:
        print(ex)
        # logger.error("Error Writing Curated Stream to Console",extra={"http.request.body.content": ex})
    

def write_encriched_df_es_sink(streamDF):
    pass
    """
        By using foreach and foreachBatch we can write custom logic to store data. 
        foreach performs custom write logic on each row and 
        foreachBatch performs custom write logic on each micro-batch.

        Using foreachBatch, we write each micro-batch to storage defined in our custom logic. 
        In this case, we store the output of our streaming application to Elastic.

        The code pattern streamingDF.writeStream.foreachBatch(...) 
        allows you to apply batch functions to the output data of every micro-batch of the streaming query. Functions used with foreachBatch take two parameters:

        A DataFrame that has the output data of a micro-batch.

        The unique ID of the micro-batch.
    """

    # write to ES
    # define function to write micro batches

# https://medium.com/globant/multiple-sinks-in-spark-structured-streaming-38997d9a59e9
    
# https://canadiandataguy.medium.com/simplifying-real-time-data-processing-with-spark-streamings-foreachbatch-6bb14b426e74
# very key - https://supertype.ai/notes/streaming-data-processing-part-3/


def write_df_to_es(df,epochid):
    df.write \
    .mode("append") \
    .format("es") \
    .option("es.nodes", es_domain) \
    .option("es.port",es_port ) \
    .option("es.net.http.auth.user", es_user) \
    .option("es.net.http.auth.pass", es_password) \
    .option("es.nodes.wan.only","true") \
    .option("es.mapping.id", "data_key")   \
    .option("es.resource",es_index) \
    .option("checkpointLocation", "/Users/addyclement/spark/checkpointdir") \
    .save()
#    .option("checkpointLocation", checkpointloc) \
    
def write_enriched_df_to_sink(df,appname):

    try: 
        final_stream_df = df.writeStream.foreachBatch(write_df_to_es).queryName(appname).start()

        # print("status :", final_stream_df.isStreaming)

        final_stream_df.awaitTermination()

    except Exception as ex :
        logger.error("Error Writing Curated Stream to Elastic Sink",extra={"http.request.body.content": ex})
    
"""
    def foreach_batch(stream_df_04, epoch_id):
	 stream_df_04.write \
	.mode("append") \
	.format("es") \
	.option("es.nodes", "https://search-bexley-shopping-xxxxxxxx.eu-west-2.es.amazonaws.com") \
	.option("es.port", "443") \
	.option("es.net.http.auth.user", es_user) \
	.option("es.net.http.auth.pass", es_password) \
	.option("es.nodes.wan.only","true") \
      .option("es.mapping.id", data_key) \     
      .option("es.mapping.date.rich","false") \
      .option("checkpointLocation", "/logs/kafka/bexley-logs") \
	.option("es.resource","bexley_orders_01") \
	.save()

stream_df_05 = stream_df_04.writeStream.foreachBatch(foreach_batch).queryName("bexley-stream-01").start()

"""

if __name__ == "__main__":
    main()
