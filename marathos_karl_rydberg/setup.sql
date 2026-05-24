CREATE CATALOG IF NOT EXISTS marathos;

CREATE SCHEMA IF NOT EXISTS marathos.default;
CREATE SCHEMA IF NOT EXISTS marathos.bronze;
CREATE SCHEMA IF NOT EXISTS marathos.silver;
CREATE SCHEMA IF NOT EXISTS marathos.gold;
CREATE SCHEMA IF NOT EXISTS marathos.information_schema;

CREATE VOLUME IF NOT EXISTS marathos.default.raw;