# Paddy Statistics Lakehouse – Architecture

This document describes the end-to-end architecture of the Paddy Statistics Lakehouse project on AWS.

It explains:

- The main components (S3, Lambda, Athena, IAM).
- How data flows from source files to Silver.
- How the system is intended to be used and extended.

---

## 1. High-Level Overview

The project ingests static CSV/Excel files containing Sri Lankan paddy statistics (Maha/Yala seasons), cleans and normalises them with a Lambda function, writes partitioned Parquet to S3 (Silver layer), and exposes the data via Athena for analysis.

High-level flow:

1. Source CSV/Excel files are stored in an S3 bucket.
2. A Python AWS Lambda (`lambda_function.py`) reads each file, cleans/normalises it, and writes Silver data back to S3 as Parquet.
3. An Athena external table (`paddy_db.paddy_stats`) points at the Silver Parquet data and uses Hive partitions for efficient querying.
4. (Planned) Gold views/tables will sit on top of Silver for BI-friendly consumption.

---

## 2. Components

### 2.1 S3 – Data Lake Storage

- **Bucket: khalid-crop-yield-proj
- **Key prefixes:**
  - Raw / incoming files: s3://khalid-crop-yield-proj/bronze/paddy_stats/
  - Silver layer (configured via `SILVER_PREFIX`, default: `silver/paddy_stats/`):
  
    - `silver/paddy_stats/metric_name=<metric_name>/season=<Maha|Yala|unknown>/harvest_year=<YYYY|unknown>/part-<checksum>.parquet`

S3 acts as the **data lake**:

- Raw/static source files live here.
- Silver layer Parquet files are written here.
- Athena reads directly from the Silver prefix.

---

### 2.2 AWS Lambda – Ingestion & Silver Transformation

- **File:** `src/lambda/lambda_function.py`
- **Runtime:** Python
- **Libraries:** `boto3`, `pandas`, `re`, `hashlib`, `io`, `os`, `json`
- **Configuration:**
  - `DATA_BUCKET` – S3 bucket containing raw files and where Silver is written.
  - `SILVER_PREFIX` – S3 prefix for Silver layer (default: `silver/paddy_stats/`).

**Responsibilities:**

1. **Read source file from S3**
   - Triggered either by:
     - S3 event (`Records` in event), or
     - Manual/event-based invocation with `{"s3_key": "<path/to/file>"}` and `DATA_BUCKET` set.

2. **Detect captions and headers**
   - Uses functions:
     - `detect_caption_row_wide`
     - `detect_header_idx`
     - `has_two_level_header`
   - Handles messy multi-row headers and caption rows.

3. **Flatten column headers**
   - Uses `flatten_columns` to convert multi-level headers to a single row of names.

4. **Remove annotation rows**
   - Uses `is_annotation_row` to drop footnote-style rows with long text and no numeric content.

5. **Identify district column**
   - Searches for a column containing "district".
   - If none found, assigns `district = 'ALL_ISLAND'` to all rows.

6. **Reshape to long / tidy format**
   - Uses `pd.melt()` to convert wide year/subcategory columns into rows:
     - `district`
     - `year_col` (temporary)
     - `value`

7. **Derive year and subcategory**
   - `split_year_and_subcat` → `year_label`, `subcategory`
   - `harvest_year_from` → `harvest_year` (handles pairs like `1978/1979` with season awareness).

8. **Unit and metric detection**
   - `parse_from_name(key)` → `metric_name`, default unit, season (from file name).
   - `maybe_unit_from_caption(cap_text, unit)` → refine unit from caption if available.

9. **Clean numeric values**
   - Strip commas and whitespace.
   - Convert to numeric via `pd.to_numeric(errors="coerce")`.

10. **Write partitioned Parquet to Silver**
    - For each `harvest_year` group, write to:
      - `silver/paddy_stats/metric_name=<metric_name>/season=<season or 'unknown'>/harvest_year=<YYYY or 'unknown'>/part-<checksum>.parquet`

**Output schema per row (Silver):**

- `district`
- `value`
- `year_label`
- `subcategory`
- `metric_name`
- `season`
- `unit`
- `harvest_year`

---

### 2.3 Athena – Query Layer

- **Workgroup:** `paddy_wg`
- **Database:** `paddy_db`
- **Silver table:** `paddy_stats` (external)

The external table maps to the Silver S3 prefix and uses **Hive partitions**:

- `metric_name`
- `season`
- `harvest_year`

This allows:

- Filtering by metric (e.g. Production only).
- Seasonal comparisons (Maha vs Yala).
- Time-based analysis by year.

**Important:**

- After new partitions are written to Silver, you may need to run:
  - `MSCK REPAIR TABLE paddy_db.paddy_stats;`
  - or `ALTER TABLE ... ADD PARTITION ...` if doing it manually.

---

## 3. Data Flow Summary

End-to-end flow:

1. **Upload source file**  
   A CSV/Excel file (Maha or Yala; Production/Extent/Yield) is uploaded to the `DATA_BUCKET`.

2. **Invoke Lambda**
   - Either via S3 event trigger, or manually with a payload specifying `s3_key`.

3. **Lambda processing**
   - Reads file from S3.
   - Detects captions and headers, flattens columns.
   - Drops annotation rows.
   - Identifies `district` column (or substitutes `ALL_ISLAND`).
   - Melts to long format.
   - Derives `year_label`, `subcategory`, `harvest_year`.
   - Detects metric, unit, and season from file name + caption.
   - Cleans numeric values.
   - Writes partitioned Parquet to the Silver prefix.

4. **Catalog update**
   - Athena’s Glue Data Catalog is updated (via `MSCK REPAIR TABLE` or `ALTER TABLE ... ADD PARTITION`) so new partitions are discoverable.

5. **Querying in Athena**
   - Analysts and you can query `paddy_db.paddy_stats` in the `paddy_wg` workgroup.
   - Queries can filter by `metric_name`, `season`, and `harvest_year`.

6. **(Planned) Gold Layer**
   - Future: a Gold view/table (e.g. `gld_vw_paddy_stats` or `gld_paddy_stats`) will be created on top of `paddy_stats` for BI and dashboard consumption.

---

## 4. Operational Characteristics

- **Static dataset:**  
  - Source files are static paddy statistics.  
  - No periodic ingestion or scheduling (no EventBridge / cron jobs yet).

- **Execution model:**  
  - Lambda can be invoked on-demand per file.
  - Suitable for “backfill once and query many times” workflows.

- **Logging & debugging:**
  - Lambda prints debug logs (e.g. dropped annotation rows, final column list).
  - These logs are visible in CloudWatch for troubleshooting.
