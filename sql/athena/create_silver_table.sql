CREATE EXTERNAL TABLE IF NOT EXISTS paddy_db.paddy_stats (
  district     string,
  year_label   string,
  subcategory  string,
  value        double,
  unit         string
)
PARTITIONED BY (
  metric_name  string,
  season       string,
  harvest_year int
)
STORED AS PARQUET
LOCATION 's3://khalid-crop-yield-proj/silver/paddy_stats/';

MSCK REPAIR TABLE paddy_db.paddy_stats;

