def write_to_sink(dataframe, format_type="avro", write_mode="overwrite", path_location="data-sink/avro",
                  partition_cols=None):
    if partition_cols is not None:
        dataframe.write \
            .format(format_type) \
            .mode(write_mode) \
            .option("path", path_location) \
            .option("maxRecordsPerFile",10000) \
            .partitionBy(*partition_cols) \
            .save()
    else:
        dataframe.write \
            .format(format_type) \
            .mode(write_mode) \
            .option("path", path_location) \
            .save()


def write_to_sink_table(dataframe,table_name, format_type="avro", write_mode="overwrite",partition_cols=None,number_of_buckets=5):
    if partition_cols is not None and number_of_buckets is not None:
        # if the values in the partition cols are mostly non-repeating, the storing the data results in a lengthy
        # folder structure to avoid this we can use the concept of bucketing
        dataframe.write \
            .format(format_type) \
            .mode(write_mode) \
            .bucketBy(number_of_buckets,*partition_cols) \
            .sortBy(*partition_cols) \
            .saveAsTable(table_name)

    elif partition_cols is not None:
        dataframe.write \
            .format(format_type) \
            .mode(write_mode) \
            .option("maxRecordsPerFile", 10000) \
            .partitionBy(*partition_cols) \
            .saveAsTable(table_name)
    else:
        dataframe.write \
            .format(format_type) \
            .mode(write_mode) \
            .saveAsTable(table_name)
