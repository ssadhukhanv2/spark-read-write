def read():
    print("Read")


def load_csv_to_data_frame(spark_session, file_path, schema_info=None):
    # Doc on DataFrameReader.json: https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrameReader.json.html

    # Instead of using the .format(), here we are using .csv() to load the csv file
    # data_frame_csv = spark_session.read \
    #     .option("header", "true") \
    #     .option("inferSchema", "true") \
    #     .csv(files_path)

    # data_frame_csv = spark_session.read.format("csv") \
    #     .option("header", "true") \
    #     .load(files_path)

    # explicit way of loading data_frames
    # .format(): a lot of community formats like JSON, AVRO, CSV, Cassandra, MongoDB, AVRO, XML, HBase, Redshift are supported by .format() method
    # .option(path,files_path): files_path is the path to the directory where the files are present,
    #   path may be a file name like "data/flight-time.csv"
    #   path may be a folder name like "data/" , this will pick up all files within the directory(may be confusing so assume that all files within the directory will be picked up instead of a specific format)
    #   path may be a pattern like "data/flight*.csv" , this will pick up all files matching the pattern
    # .option("mode", read_mode): mode is used to specify the read mode. There are 3 read modes in spark
    #   PERMISSIVE -> the PERMISSIVE read mode inserts a row with all null columns whenever it encounters a corrupted record and places the corrupted record in a string column called _corrupt_record
    #   DROPMALFORMED -> the DROPMALFORMED read mode simple ignores the malformed record, thus drops it while reading into dataframe
    #   FAILFAST -> the FAILFAST read mode raises an exception and terminates immediately whenever a corrupted record is encountered
    # .schema: schema may be implicit, explicit or inferschema
    #   Infer Schema -> if we choose not to provide an explicit schema for our dataset dataframereader infers the schema
    #   Explicit -> if we choose not to provide an explicit schema for our dataset dataframereader infers the schema
    #   Implicit -> Some of the data sources like AVRO and PARQUET comes with an well defined schema, for these we don't need to define a schema
    # .load(): method is called to create the dataframe
    data_frame_csv = None

    if schema_info is not None:
        data_frame_csv = spark_session.read.format("csv") \
            .option("header", "true") \
            .option("path", file_path) \
            .option("mode", "FAILFAST") \
            .option("dateFormat", "M/d/y") \
            .schema(schema_info) \
            .load()
    else:
        data_frame_csv = spark_session.read.format("csv") \
            .option("header", "true") \
            .option("path", file_path) \
            .option("inferSchema", "true") \
            .option("mode", "FAILFAST") \
            .load()

    return data_frame_csv


def load_json_to_data_frame(spark_session, file_path, schema_info=None):
    # with .json we don't need the .option("inferSchema", "true") \ option as spark will will always infer schema for json formats
    # whle reading from json formats the columns in the dataframe are sorted in alphabetical order
    data_frame_json = None
    if schema_info is not None:
        data_frame_json = spark_session.read.format("json") \
            .option("header", "true") \
            .option("path", file_path) \
            .option("mode", "FAILFAST") \
            .option("dateFormat","M/d/y") \
            .schema(schema_info) \
            .load()
    else:
        data_frame_json = spark_session.read.format("json") \
            .option("header", "true") \
            .option("path", file_path) \
            .option("mode", "FAILFAST") \
            .load()
    return data_frame_json


def load_parquet_to_data_frame(spark_session, file_path):
    # files like .parquet and .avro have well-defined schema along with the data within the file, so spark can
    # implicitly infer the schema when we are using these formats, so we should use .parquet & .avro format as often as possible
    data_frame_parquet = spark_session.read.format("parquet") \
        .option("header", "true") \
        .option("mode", "FAILFAST") \
        .load(file_path)
    return data_frame_parquet
