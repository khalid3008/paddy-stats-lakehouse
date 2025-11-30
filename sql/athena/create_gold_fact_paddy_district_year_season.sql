CREATE TABLE paddy_db.fact_paddy_district_year_season
WITH (
  format = 'PARQUET',
  external_location = 's3://khalid-crop-yield-proj/gold/paddy_stats/fact_paddy_district_year_season/',
  partitioned_by = ARRAY['harvest_year', 'season']
) AS
WITH base AS (
    SELECT
        district,
        harvest_year,
        season,
        metric_name,
        UPPER(COALESCE(subcategory, '')) AS subcategory,
        value
    FROM paddy_db.paddy_stats_clean
    WHERE harvest_year IS NOT NULL
      AND district IS NOT NULL          -- drops HIGHLAND PADDY, SRI LANKA, DISTRICT that were set to NULL
      AND TRIM(district) <> ''          -- no blank districts
      AND UPPER(district) <> 'SRI LANKA'
),

pivoted AS (
    SELECT
        district,
        -- Use only TOTAL / ALL for extent metrics
        SUM(CASE 
              WHEN LOWER(metric_name) LIKE '%sown%' 
                   AND subcategory IN ('TOTAL')
              THEN value 
            END) AS sown_extent_ha,

        SUM(CASE 
              WHEN LOWER(metric_name) LIKE '%harvest%' 
                   AND subcategory IN ('TOTAL')
              THEN value 
            END) AS harvested_extent_ha,

        -- Average yield (reported) at TOTAL / ALL level
        SUM(CASE 
              WHEN LOWER(metric_name) LIKE '%average%' 
                   AND LOWER(metric_name) LIKE '%yield%' 
                   AND subcategory IN ('TOTAL')
              THEN value 
            END) AS yield_reported_kg_per_ha,

        -- Production: use all rows (usually already "total" level)
        SUM(CASE 
              WHEN LOWER(metric_name) LIKE '%production%' 
              THEN value 
            END) AS production_mt,
        harvest_year,
        season
    FROM base
    GROUP BY
        district,
        harvest_year,
        season
)

SELECT * FROM pivoted;
