CREATE TABLE paddy_db.fact_paddy_national_year_season
WITH (
  format = 'PARQUET',
  external_location = 's3://khalid-crop-yield-proj/gold/paddy_stats/fact_paddy_national_year_season/',
  partitioned_by = ARRAY['harvest_year', 'season']
) AS
WITH base AS (
    SELECT
        harvest_year,
        season,
        sown_extent_ha,
        harvested_extent_ha,
        production_mt,
        yield_reported_kg_per_ha
    FROM paddy_db.fact_paddy_district_year_season
),

agg AS (
    SELECT
        SUM(sown_extent_ha)      AS national_sown_extent_ha,
        SUM(harvested_extent_ha) AS national_harvested_extent_ha,
        SUM(production_mt)       AS national_production_mt,

        -- Weighted reported yield using harvested extent
        CASE
            WHEN SUM(harvested_extent_ha) > 0 THEN
                SUM(yield_reported_kg_per_ha * harvested_extent_ha)
                / SUM(harvested_extent_ha)
        END AS national_yield_reported_kg_per_ha,
        CAST(current_timestamp AS timestamp) AS created_at,
        harvest_year,
        season
    FROM base
    GROUP BY
        harvest_year,
        season
)

SELECT * FROM agg;
