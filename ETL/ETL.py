from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, expr, lit
import datetime

if __name__ == "__main__":
    # Get the current date
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")

    # Split year, month, and day from current_date
    run_time = current_date.split("-")
    year = run_time[0]
    month = run_time[1]
    day = run_time[2]

    # Initialize Spark session with Hive support
    spark = SparkSession.builder \
        .appName("ETL Report") \
        .config("hive.metastore.uris", "thrift://hadoop-namenode:9083") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.serialization.extend.nesting.levels", "10") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()

    # Read static data to infer schema
    static_df = spark.read.json("hdfs://hadoop-namenode:8020/data/*")

    # Read streaming data from HDFS
    streaming_df = spark.readStream \
        .schema(static_df.schema) \
        .json("hdfs://hadoop-namenode:8020/data/*")

    # Apply the same transformations on the streaming DataFrame
    id_column = col("id")
    exploded_df = streaming_df.select(id_column, explode("transactions.entry.entry").alias("entry"))
    filtered_df = exploded_df.select(
        id_column,
        expr("filter(entry.resource.subscriberId, x -> x is not null)[0]").alias("SubscriberId"),
        expr("filter(entry.resource.identifier, x -> x is not null)").alias("Identifier"),
        expr("filter(entry.resource.name, x -> x is not null)[0]").alias("Name"),
        expr("filter(entry.resource.gender, x -> x is not null)[0]").alias("Gender"),
        expr("filter(entry.resource.birthDate, x -> x is not null)[0]").alias("BirthDate"),
        expr("filter(entry.resource.address, x -> x is not null)[0]").alias("Address"),
        expr("filter(entry.resource.name, x -> x is not null)[1]").alias("Doctor"),
        expr("filter(entry.resource.name, x -> x is not null)[3]").alias("Faculty"),
        expr("filter(entry.resource.name, x -> x is not null)[4]").alias("Room"),
        expr("filter(entry.resource.name, x -> x is not null)[2]").alias("HospitalName"),
        expr("filter(entry.resource.reasonCode, x -> x is not null)").alias("Diagnose"),
        expr("filter(entry.resource.requester, x -> x is not null)").alias("Requester"),
        expr("filter(entry.resource.code, x -> x is not null)").alias("Service")
    )

    select_df1 = filtered_df.select(
        id_column,
        expr("SubscriberId"),
        expr("Identifier[0].value[0]").alias("IDPatient"),
        expr("Identifier[1].value[0]").alias("Medical Records"),
        expr("GET_JSON_OBJECT(Name, '$[0].text')").alias("PatientName"),
        expr("Gender"),
        expr("BirthDate"),
        expr("Address[0].text").alias("Address"),
        expr("GET_JSON_OBJECT(Doctor, '$[0].text')").alias("DoctorName"),
        expr("Faculty"),
        expr("Room"),
        expr("HospitalName"),
        expr("Diagnose[0].text[0]").alias("Diagnose"),
        expr("Requester.display[0]").alias("Requester"),
        expr("transform(Service.coding, x -> x[0].display)").alias("Service")
    )

    # Add the current year, month, and day as new columns
    select_df1 = select_df1.withColumn("year", lit(year)) \
                           .withColumn("month", lit(month)) \
                           .withColumn("day", lit(day))

    # Define a function to write each micro-batch to Hive
    def write_to_hive(batch_df, batch_id):
        batch_df.write.format("hive") \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .saveAsTable("report.report5")

    # Start the streaming query
    query = select_df1.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_hive) \
        .option("checkpointLocation", "/home/hadoop/checkpointLocation/") \
        .start() \
        .awaitTermination()

