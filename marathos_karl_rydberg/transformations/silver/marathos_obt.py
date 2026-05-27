from pyspark import pipelines as dp 
from pyspark.sql.functions import coalesce, lit, when, col, to_date, regexp_extract, concat_ws, upper
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
          .drop("temp_start", "event_dates").withColumn("athlete_country", when(upper(col("athlete_country")) == "XXX", None)
          .when(upper(col("athlete_country")) == "SVE", "SWE").otherwise(upper(col("athlete_country")))).withColumn("athlete_year_of_birth", when((col("year_of_event") - col("athlete_year_of_birth") < 15) |
          (col("year_of_event") - col("athlete_year_of_birth") > 100), None)
          .otherwise(col("athlete_year_of_birth").cast("integer"))).withColumn("athlete_age_category",
    when(col("athlete_age_category") == "F35", "W35")
    .otherwise(col("athlete_age_category"))).withColumn("athlete_average_speed",
                when(col("athlete_average_speed").isNull(), None)
                .when(col("athlete_average_speed").rlike(r"^\d{2}:\d{2}:\d{2}$"), None)
                .otherwise(col("athlete_average_speed").cast("double")))
          .withColumn("athlete_average_speed",
                when(
                    (col("athlete_average_speed") < 2.0) |
                    (col("athlete_average_speed") > 30.0), None)
                .otherwise(col("athlete_average_speed")))
    )