from pyspark import pipelines as dp


BASE_DIR = "/Volumes/marathos/default/raw"
# raw_file_path = /Volumes/marathos/default/raw/TWO_CENTURIES_OF_UM_RACES.csv

# parse schema from csv file and use it for the streaming table
# as readStream processes data continously and can't look ahead of time to infer schema

schema = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{BASE_DIR}/TWO_CENTURIES_OF_UM_RACES.csv")
    .schema
)

# https://docs.delta.io/delta-column-mapping/


@dp.table(
    name="marathos.bronze.raw_races",
    # enable column mapping to handle invalid characters in original column names
    comment="Raw data from marathos csv file",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def raw_races():
    return (
        spark.readStream.format("csv")
        .options(header="true", inferSchema="true", encoding="UTF-8")
        .schema(schema)
        .load(f"{BASE_DIR}")
    )