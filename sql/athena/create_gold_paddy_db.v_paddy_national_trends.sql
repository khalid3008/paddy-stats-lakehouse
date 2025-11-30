CREATE OR REPLACE VIEW paddy_db.v_paddy_national_trends AS
SELECT
    harvest_year,
    season,
    national_sown_extent_ha,
    national_harvested_extent_ha,
    national_production_mt,
    national_yield_reported_kg_per_ha,

    LAG(national_production_mt) OVER (
        PARTITION BY season
        ORDER BY harvest_year
    ) AS prev_production_mt,

    CASE
        WHEN LAG(national_production_mt) OVER (
                 PARTITION BY season
                 ORDER BY harvest_year
             ) > 0
        THEN
            100.0 * (
                national_production_mt
                - LAG(national_production_mt) OVER (
                    PARTITION BY season
                    ORDER BY harvest_year
                  )
            )
            / LAG(national_production_mt) OVER (
                PARTITION BY season
                ORDER BY harvest_year
              )
    END AS production_yoy_pct,

    LAG(national_yield_reported_kg_per_ha) OVER (
        PARTITION BY season
        ORDER BY harvest_year
    ) AS prev_yield_kg_per_ha,

    CASE
        WHEN LAG(national_yield_reported_kg_per_ha) OVER (
                 PARTITION BY season
                 ORDER BY harvest_year
             ) > 0
        THEN
            100.0 * (
                national_yield_reported_kg_per_ha
                - LAG(national_yield_reported_kg_per_ha) OVER (
                    PARTITION BY season
                    ORDER BY harvest_year
                  )
            )
            / LAG(national_yield_reported_kg_per_ha) OVER (
                PARTITION BY season
                ORDER BY harvest_year
              )
    END AS yield_yoy_pct
FROM paddy_db.fact_paddy_national_year_season;