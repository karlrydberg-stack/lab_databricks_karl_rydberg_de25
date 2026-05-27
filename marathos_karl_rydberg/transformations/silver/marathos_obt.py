from pyspark import pipelines as dp 
from pyspark.sql.functions import coalesce, lit, when, col, to_date, regexp_extract, concat_ws
from utils.utils import rename_columns_to_snake_case

@dp.table(name="marathos.silver.cleaned_marathos_obt",
          comment="Cleaned marathos data",
          table_properties={
              "delta.columnMapping.mode": "name",
              "delta.minReaderVersion": "2",
              "delta.minWriterVersion": "5"
          })
def cleaned_marathos():
    df = rename_columns_to_snake_case(spark.sql("SELECT * FROM STREAM marathos.bronze.raw_races"))
    return (
        df.withColumn("temp_start",
                concat_ws(".",
                    when(regexp_extract(col("event_dates"), r"^(\d{2})", 1) == "00", "01")
                    .otherwise(regexp_extract(col("event_dates"), r"^(\d{2})", 1)),
                    when(regexp_extract(col("event_dates"), r"\.(\d{2})\.\d{4}$", 1) == "00", "01")
                    .otherwise(regexp_extract(col("event_dates"), r"\.(\d{2})\.\d{4}$", 1)),
                    regexp_extract(col("event_dates"), r"(\d{4})$", 1)
                ))
          .withColumn("event_start_date", to_date(col("temp_start"), "dd.MM.yyyy"))
          .drop("temp_start", "event_dates")
    )