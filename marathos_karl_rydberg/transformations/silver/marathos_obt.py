from pyspark import pipelines as dp 
from pyspark.sql.functions import coalesce, lit, when, col, to_timestamp
from utils.utils import rename_columns_to_snake_case
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_timestamp

@dp.table(name="marathos.silver.cleaned_marathos_obt",
          comment="Cleaned marathos data",
        table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
    })
def cleaned_supply_chain():
    return (
        rename_columns_to_snake_case(spark.sql("SELECT * FROM STREAM marathos.bronze.raw_races"))
        .withColumn("event_dates", to_timestamp(col("event_dates"), "dd.MM.yyyy"))
    ).drop("event_dates")