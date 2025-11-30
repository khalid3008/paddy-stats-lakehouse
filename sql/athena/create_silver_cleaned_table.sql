CREATE TABLE paddy_db.paddy_stats_clean
WITH (
  format = 'PARQUET',
  external_location = 's3://khalid-crop-yield-proj/silver/paddy_stats_clean/',
  partitioned_by = ARRAY['season', 'harvest_year']
) AS
SELECT
    district AS district_raw,

    CASE 
        WHEN district IS NULL OR TRIM(district) = '' THEN NULL
        WHEN UPPER(REPLACE(district, ' ', '')) IN (
            'KILINOCHCHI', 'KILLINOCHCHI', 'KILINOCHCHIYA'
        ) THEN 'KILINOCHCHI'
        WHEN UPPER(REPLACE(district, ' ', '')) IN (
            'MAHAWELI''H'''
        ) THEN 'MAHAWELI ''H'''
        WHEN UPPER(TRIM(district)) IN ('HIGHLAND PADDY', 'SRI LANKA', 'DISTRICT')
          THEN NULL
        ELSE UPPER(TRIM(district))
    END AS district,

    year_label,
    subcategory,
    value,
    unit,
    metric_name,
    season,
    harvest_year
FROM paddy_db.paddy_stats
WHERE harvest_year IS NOT NULL;