spark.sql("CREATE CATALOG IF NOT EXISTS marathos")
spark.sql("CREATE SCHEMA IF NOT EXISTS marathos.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS marathos.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS marathos.gold")