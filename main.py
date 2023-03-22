# This is a sample Python script.
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType

from lib import utils
import reader_spark as reader
import writer_spark as writer


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def hello_spark_without_conf_file(app_name):
    spark_session = SparkSession.builder.appName(app_name) \
        .master("local[*]") \
        .getOrCreate()

    data_list = [("Ravi", 28), ("David", 45), ("Abdul", 37)]
    data_frame = spark_session.createDataFrame(data_list).toDF("Name", "Age")
    data_frame.show(n=3)
    spark_session.stop()


def read_from_different_data_sources():
    spark_session = utils.create_spark_session_using_config_file("config/spark.conf")
    flight_schema_struct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])
    flight_schema_ddl = '''FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
              ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
              WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT'''

    print("reading from csv infer schema=true")
    flight_time_csv_data_frame = reader.load_csv_to_data_frame(spark_session, "data-source/flight-time.csv")
    flight_time_csv_data_frame.show(n=5)
    print(flight_time_csv_data_frame.count())
    print(f"csv schema: {flight_time_csv_data_frame.schema.simpleString()}")

    print("reading from csv explicit schema specified programmatically")
    flight_time_csv_data_frame = reader.load_csv_to_data_frame(spark_session, "data-source/flight-time.csv",
                                                               schema_info=flight_schema_struct)
    flight_time_csv_data_frame.show(n=5)
    print(flight_time_csv_data_frame.count())
    print(f"csv schema: {flight_time_csv_data_frame.schema.simpleString()}")

    print("reading from csv explicit schema specified using ddl")
    flight_time_csv_data_frame = reader.load_csv_to_data_frame(spark_session, "data-source/flight-time.csv",
                                                               schema_info=flight_schema_ddl)
    flight_time_csv_data_frame.show(n=5)
    print(flight_time_csv_data_frame.count())
    print(f"csv schema: {flight_time_csv_data_frame.schema.simpleString()}")

    print("reading from json without passing any schema information")
    flight_time_json_data_frame = reader \
        .load_json_to_data_frame(spark_session=spark_session, file_path="data-source/flight-*.json")
    flight_time_json_data_frame.show(n=5)
    print(f"json schema: {flight_time_json_data_frame.schema.simpleString()}")

    print("reading from json passing any schema information as ddl")
    flight_time_json_data_frame = reader \
        .load_json_to_data_frame(spark_session=spark_session, file_path="data-source/flight-*.json",
                                 schema_info=flight_schema_ddl)
    flight_time_json_data_frame.show(n=5)
    print(f"json schema: {flight_time_json_data_frame.schema.simpleString()}")

    print("reading from parquet")
    flight_time_parquet_data_frame = reader \
        .load_parquet_to_data_frame(spark_session=spark_session, file_path="data-source/flight-time.parquet")
    flight_time_parquet_data_frame.show(n=5)
    print(f"parquet schema: {flight_time_parquet_data_frame.schema.simpleString()}")

    utils.stop_spark_session(spark_session)


def repartitioning_data_read_parquet_write_avro():
    print("Read parquet files from data-source, create 5 partition within dataframes and write data into sink as avro "
          "format")

    spark_session = utils.create_spark_session_using_config_file("config/spark.conf")
    flight_time_parquet_data_frame = reader \
        .load_parquet_to_data_frame(spark_session=spark_session, file_path="data-source/flight-time.parquet")
    flight_time_parquet_data_frame.show(n=5)
    print(
        f"Number of Dataframe partition before repartitioning: {flight_time_parquet_data_frame.rdd.getNumPartitions()}")
    flight_time_parquet_data_frame.groupBy(spark_partition_id()).count().show()

    flight_time_parquet_partitioned_data_frame = flight_time_parquet_data_frame.repartition(5)

    print(
        f"Number of Dataframe partition after repartitioning: {flight_time_parquet_partitioned_data_frame.rdd.getNumPartitions()}")
    flight_time_parquet_partitioned_data_frame.groupBy(spark_partition_id()).count().show()

    writer.write_to_sink(flight_time_parquet_partitioned_data_frame, format_type="avro", write_mode="overwrite",
                         path_location="data-sink/avro")

    utils.stop_spark_session(spark_session)


def partition_by_carrier_origin_read_parquet_write_json():
    print("Read parquet files from data-source, create 5 partition within dataframes and write data into sink as avro "
          "format")

    spark_session = utils.create_spark_session_using_config_file("config/spark.conf")
    flight_time_parquet_data_frame = reader \
        .load_parquet_to_data_frame(spark_session=spark_session, file_path="data-source/flight-time.parquet")
    flight_time_parquet_data_frame.show(n=5)
    print(
        f"Number of Dataframe partition before repartitioning: {flight_time_parquet_data_frame.rdd.getNumPartitions()}")
    flight_time_parquet_data_frame.groupBy(spark_partition_id()).count().show()

    print(
        f"Number of Dataframe partition after repartitioning: {flight_time_parquet_data_frame.rdd.getNumPartitions()}")
    flight_time_parquet_data_frame.groupBy(spark_partition_id()).count().show()

    partition_cols = "OP_CARRIER", "ORIGIN"

    writer.write_to_sink(flight_time_parquet_data_frame, format_type="json", write_mode="overwrite",
                         path_location="data-sink/json", partition_cols=partition_cols)

    utils.stop_spark_session(spark_session)


def read_data_and_load_to_spark_table():
    print("Read parquet files from data-source and load data in a spark table")

    spark_session = utils.create_spark_session_using_config_file("config\spark.conf", enable_hive_support=True)

    flights_time_dataframe = reader.load_parquet_to_data_frame(spark_session=spark_session,
                                                               file_path="data-source/flight-time.parquet")
    spark_session.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark_session.catalog.setCurrentDatabase("AIRLINE_DB")

    partition_cols = "OP_CARRIER", "ORIGIN"

    writer.write_to_sink_table(dataframe=flights_time_dataframe, table_name="flight_data_tbl", format_type="csv",
                               write_mode="overwrite", partition_cols=partition_cols,number_of_buckets=10)

    print(spark_session.catalog.listTables("AIRLINE_DB"))


if __name__ == '__main__':
    # hello_spark_without_conf_file("Spark Program to demo use of file and database table based data sources and sinks")
    # read_from_different_data_sources()
    # repartitioning_data_read_parquet_write_avro()
    # partition_by_carrier_origin_read_parquet_write_json()
    read_data_and_load_to_spark_table()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
