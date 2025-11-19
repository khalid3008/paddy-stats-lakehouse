# Paddy Statistics Data Model

This document describes the actual Silver and planned Gold data models for the Paddy Statistics Lakehouse project.  
It reflects the exact schema produced by the Lambda (`lambda_function.py`) and the Athena table created in:

- Workgroup: paddy_wg  
- Database: paddy_db  
- Table: paddy_stats  

---

## 1. Source → Silver Overview

The Lambda reads CSV/Excel files containing Sri Lanka paddy statistics, detects headers/captions, flattens multi-row headers, identifies district columns, reshapes the data into long format, derives usable years, and writes Parquet data to Silver.

### Silver Output S3 Path

```
s3://<DATA_BUCKET>/silver/paddy_stats/
    metric_name=<metric_name>/
        season=<Maha|Yala|unknown>/
            harvest_year=<YYYY|unknown>/
                part-<checksum>.parquet
```

### Silver Athena Table

- Database: paddy_db  
- Table: paddy_stats  
- External table pointing at the partitioned Parquet data above.

---

## 2. Silver Layer Model (paddy_db.paddy_stats)

### Grain

Each row represents:

**district – season – year_label – subcategory – harvest_year – metric_name – value**

### Actual Columns Written by Lambda

| Column Name    | Description |
|----------------|-------------|
| district       | Cleaned district name. If no district column found, Lambda assigns `ALL_ISLAND`. |
| value          | Numeric metric value, cleaned via comma removal + `to_numeric(errors="coerce")`. |
| year_label     | The original year label from the header (e.g., `1978/1979`, `2020`). |
| subcategory    | Optional: extracted from headers like `1978/1979_Major`. Can be NULL. |
| metric_name    | Derived from filename (e.g., `Production`, `Average_Yield`, `Sown_Extent`, etc.). |
| season         | Derived from filename: `Maha`, `Yala`, or `None` (written to S3 as `unknown`). |
| unit           | Unit inferred from filename or caption (e.g., `Kg/Ha`, `000 MT`, `Hectares`). |
| harvest_year   | Numeric year derived from `year_label` using `harvest_year_from()`. May be NULL if unparseable. |

### Partition Columns (Actual)

The folder structure creates these partitions in Athena:

| Partition Column | Source |
|------------------|--------|
| metric_name      | From filename |
| season           | From filename |
| harvest_year     | Derived year |

Partition layout:

```
metric_name=<...>/season=<...>/harvest_year=<...>/
```

---

## 3. Key Transformations in Silver

The Lambda performs the following:

- **Caption / header detection** using regex patterns and top-row scanning.
- **Flattening of multi-row headers** (`flatten_columns()`).
- **Dropping annotation rows** using `is_annotation_row()`.
- **District identification**, with fallback to `ALL_ISLAND`.
- **Melting** wide files into long format using `pd.melt()`.
- **Splitting year_label + subcategory** via `split_year_and_subcat()`.
- **Deriving harvest_year** using `harvest_year_from()` with season awareness.
- **Numeric cleaning** (`value` field).
- **Unit inference** with caption override via `maybe_unit_from_caption()`.

The final DataFrame written to Parquet **drops only** the temporary column `year_col`.

---

## 4. Planned Gold Layer Model

The Gold layer will be built on top of `paddy_stats` to simplify BI and analytics.  
Two options:

### Option A — Gold View
A view `gld_vw_paddy_stats` that selects and reshapes Silver fields.

### Option B — Materialised Gold Table
A CTAS-created table `gld_paddy_stats` stored in `s3://.../gold/paddy_stats/`.

### Proposed Gold Columns

| Column Name    | Description |
|----------------|-------------|
| district       | District |
| season         | Maha / Yala |
| harvest_year   | Year used for time series |
| metric_name    | Metric identifier |
| metric_value   | Same as `value` but optionally renamed for clarity |
| unit           | Unit of measure |
| subcategory    | Optional (Major, Minor, Total, etc.) |

The Gold layer may drop `year_label` unless needed for reporting, or keep both.

---

## 5. Dimensional Interpretation (Logical)

For design and interview clarity, the model maps to:

### Fact
**Fact_PaddyStats**  
- value

### Dimensions  
- Dim_District (district)  
- Dim_Season (season)  
- Dim_Year (harvest_year)  
- Dim_Metric (metric_name, unit, subcategory)

Even though these are not yet separate tables, the structure supports classical dimensional analysis.

---

## 6. Example Queries Enabled by Silver

### 6.1 Maha vs Yala production over time
```sql
SELECT
  harvest_year,
  season,
  SUM(value) AS total_production
FROM paddy_db.paddy_stats
WHERE metric_name = 'Production'
  AND season IN ('Maha', 'Yala')
  AND harvest_year IS NOT NULL
GROUP BY harvest_year, season
ORDER BY harvest_year, season;
```

### 6.2 Yield trend by district
```sql
SELECT
  district,
  harvest_year,
  value AS yield
FROM paddy_db.paddy_stats
WHERE metric_name = 'Average_Yield'
  AND harvest_year IS NOT NULL
ORDER BY district, harvest_year;
```

### 6.3 Top districts by production in a given year
```sql
SELECT
  district,
  SUM(value) AS total_production
FROM paddy_db.paddy_stats
WHERE metric_name = 'Production'
  AND harvest_year = 2020
GROUP BY district
ORDER BY total_production DESC;
```

---

## 7. Key Design Decisions (Based on Actual Lambda)

- Long-format (tidy) Silver table for extensibility.
- Partitioning by metric → season → year to optimise Athena scans.
- Intelligent handling of messy government Excel files.
- Season-aware year parsing for paired years (e.g., `1978/1979`).
- Static dataset → no scheduling / orchestration required yet.
- Gold layer planned to provide clean, BI-friendly semantics.

---
