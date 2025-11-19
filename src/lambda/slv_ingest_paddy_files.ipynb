# lambda_function.py
import os, io, re, json, hashlib
import boto3
import pandas as pd

# ────────── ENV/CLIENTS
DATA_BUCKET   = os.getenv("DATA_BUCKET")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "silver/paddy_stats/")
s3 = boto3.client("s3")

# ────────── Regex helpers
HEADER_TOKEN      = re.compile(r"\bdistrict\b", re.I)
METRIC_KEYWORDS   = re.compile(r"(yield|yeild|production|sown|harvested|extent)", re.I)
CAPTION_RE        = re.compile(r":")  # captions like "Average yield: Kg per hectare"
YEAR_PAIR         = re.compile(r"(?P<y1>(19|20)\d{2})\s*/\s*(?P<y2>(19|20)\d{2})")
YEAR_SINGLE       = re.compile(r"\b(19|20)\d{2}\b")

# ────────── Small utils
def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def parse_from_name(key: str):
    """
    Parse (metric_name, season, unit) from file name.
    Normalizes 'Yeild'→'Yield'. Provides sensible default units.
    """
    name = os.path.basename(key).lower()

    # Season
    season = "Maha" if "maha" in name else ("Yala" if "yala" in name else None)

    # Metric
    metric_name = None
    if "average" in name and ("yield" in name or "yeild" in name):
        metric_name = "Average_Yield"
    elif "production" in name:
        metric_name = "Production"
    elif "sown" in name and "extent" in name:
        metric_name = "Sown_Extent"
    elif "harvested" in name and "extent" in name:
        metric_name = "Harvested_Extent"
    else:
        # Fallback: strip extension and title-case
        metric_name = os.path.splitext(os.path.basename(key))[0].replace("-", "_").replace(" ", "_")

    # Default units by metric (override later if caption gives a better one)
    unit = None
    if metric_name == "Average_Yield":
        unit = "Kg/Ha"
    elif metric_name == "Production":
        unit = "000 MT"
    elif metric_name in ("Sown_Extent", "Harvested_Extent"):
        unit = "Hectares"

    return metric_name, season, unit

def detect_caption_row_wide(df_head: pd.DataFrame, max_rows: int = 8):
    """Scan a few top rows across all columns to find a caption-like cell."""
    rows_to_scan = min(max_rows, len(df_head))
    best = (None, None, None)
    for i in range(rows_to_scan):
        for j in range(df_head.shape[1]):
            val = str(df_head.iat[i, j]).strip()
            if not val or val.lower() in ("nan", "none"):
                continue
            if HEADER_TOKEN.search(val):
                continue
            if CAPTION_RE.search(val) or METRIC_KEYWORDS.search(val):
                best = (i, j, val)
    return best  # (row_idx, col_idx, text) or (None, None, None)

def detect_header_idx(df_head: pd.DataFrame, max_scan: int = 20) -> int:
    """Find the first row that looks like the main header (contains 'District')."""
    n = min(max_scan, len(df_head))
    for i in range(n):
        row_text = " ".join(str(x) for x in df_head.iloc[i].fillna("").tolist()).lower()
        if "district" in row_text:
            return i
    # fallback: first non-empty row
    for i in range(n):
        if any(str(x).strip() for x in df_head.iloc[i].tolist()):
            return i
    return 0

def has_two_level_header(df_head: pd.DataFrame, header_idx: int) -> bool:
    """Detect if the next row contains tokens like Major/Minor/Rainfed/Total."""
    if header_idx + 1 >= len(df_head):
        return False
    row = " ".join(str(x).strip().lower() for x in df_head.iloc[header_idx + 1].fillna("").tolist())
    tokens = {"major", "minor", "rainfed", "total"}
    return sum(1 for t in tokens if t in row) >= 2

def is_all_unnamed(col_tuple) -> bool:
    """
    For MultiIndex columns: return True if *all* non-null header levels are blank/Unnamed.
    Keep columns that have at least one meaningful label (e.g., 'District').
    """
    saw_non_null = False
    for x in col_tuple:
        if pd.isna(x):
            continue
        saw_non_null = True
        sx = str(x).strip()
        if sx and not sx.lower().startswith("unnamed"):
            return False
    # If we never saw a non-null OR all were 'Unnamed', treat as junk
    return True

def flatten_columns(cols) -> list:
    """
    Flatten pandas columns (works for both Index and MultiIndex).
    Joins non-empty, non-'Unnamed' parts with '_'. Keeps single labels as-is.
    """
    flat = []
    if isinstance(cols, pd.MultiIndex):
        for tup in cols:
            parts = []
            for x in tup:
                if pd.isna(x): 
                    continue
                sx = str(x).strip()
                if not sx or sx.lower().startswith("unnamed"):
                    continue
                parts.append(sx)
            flat.append(parts[0] if len(parts) == 1 else "_".join(parts))
    else:
        for c in cols:
            sx = str(c).strip()
            flat.append(sx if not sx.lower().startswith("unnamed") else "")
    return flat

def split_year_and_subcat(col_name: str):
    """Split '1978/1979_Major' → ('1978/1979', 'Major'). If no '_', subcat=None."""
    if "_" in col_name:
        y, sub = col_name.split("_", 1)
        return y.strip(), sub.strip()
    return col_name.strip(), None

def harvest_year_from(year_label: str, season: str | None) -> int | None:
    """Map '1978/1979'→1979 (Maha), '2022'→2022, etc."""
    s = (year_label or "").strip()
    m = YEAR_PAIR.search(s)
    if m:
        y1, y2 = int(m.group("y1")), int(m.group("y2"))
        if season == "Maha":
            return y2 if (y2 - y1) in (0, 1) else max(y1, y2)
        else:
            return y2
    m2 = YEAR_SINGLE.search(s)
    if m2:
        return int(m2.group(0))
    return None

def maybe_unit_from_caption(cap_text: str | None, default_unit: str | None):
    """
    If caption says something like 'Kg per hectare' or '000 MT', prefer that.
    Otherwise keep default.
    """
    if not cap_text:
        return default_unit
    t = cap_text.lower()
    if "kg per hectare" in t or "kg/ha" in t:
        return "Kg/Ha"
    if "000 mt" in t or "000mt" in t or "mt" in t:
        # keep '000 MT' convention for Production
        return "000 MT"
    if "hectare" in t:
        return "Hectares"
    return default_unit

def is_annotation_row(row) -> bool:
    """
    Heuristic: a row is an annotation if:
    - It has at least one long text cell with letters
    - And it has *no* numeric-looking cells (after header area)
    """
    cells = [str(x).strip() for x in row if str(x).strip()]
    if not cells:
        return False

    # Contains letters?
    text_cells = [c for c in cells if re.search(r"[A-Za-z]", c)]
    # "Looks numeric" (digits/commas/decimals)
    numeric_cells = [c for c in cells if re.fullmatch(r"[-\d,\.]+", c)]

    has_long_text = any(len(c) > 30 for c in text_cells)  # e.g. "After the 2017/2018 Maha season..."
    return has_long_text and len(numeric_cells) == 0

# ────────── Main handler
def lambda_handler(event, context):
    # Determine source
    if "Records" in event:  # S3 trigger
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key    = record["s3"]["object"]["key"]
    else:
        bucket = DATA_BUCKET
        key    = event.get("s3_key")

    if not bucket or not key:
        raise ValueError("Missing S3 bucket/key. Set DATA_BUCKET and pass {'s3_key': ...} or use S3 trigger.")

    # Read object
    obj  = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    checksum = sha256_bytes(body)

    # Parse metadata from filename
    metric_name, season, unit = parse_from_name(key)

    # Peek top rows to detect caption/header
    if key.lower().endswith(".csv"):
        df_head = pd.read_csv(io.BytesIO(body), header=None, nrows=12, dtype=str, keep_default_na=False)
    else:
        df_head = pd.read_excel(io.BytesIO(body), sheet_name=0, header=None, nrows=12, dtype=str)

    cap_r, cap_c, cap_text = detect_caption_row_wide(df_head)
    header_idx = detect_header_idx(df_head)

    start_row = header_idx
    if cap_r is not None and cap_r < header_idx:
        start_row = cap_r + 1  # header is right below caption

    two_level = has_two_level_header(df_head, start_row)
    header_spec = [start_row, start_row + 1] if two_level else start_row

    # Read full data with detected header(s)
    if key.lower().endswith(".csv"):
        df = pd.read_csv(io.BytesIO(body), header=header_spec, dtype=str, keep_default_na=False)
    else:
        df = pd.read_excel(io.BytesIO(body), sheet_name=0, header=header_spec, dtype=str)

    # For MultiIndex, drop only columns that are fully 'Unnamed'
    if isinstance(df.columns, pd.MultiIndex):
        keep_cols = [c for c in df.columns if not is_all_unnamed(c)]
        df = df.loc[:, keep_cols]

    # Remove annotation row
    mask_annot = df.apply(is_annotation_row, axis=1)
    if mask_annot.any():
        print(f"DEBUG: Dropping {mask_annot.sum()} suspected annotation rows (heuristic)")
        df = df.loc[~mask_annot].copy()

    # Flatten columns
    df.columns = flatten_columns(df.columns)
    # Drop empty column labels that may result from flattening
    df = df.loc[:, [c for c in df.columns if c and c.strip()]]

    print("DEBUG: Columns after flatten:", list(df.columns))

    # Prefer caption unit if present
    unit = maybe_unit_from_caption(cap_text, unit)

    # Identify district column robustly (no risky fallback)
    id_col = None
    for c in df.columns:
        col_norm = str(c).strip().lower()
        if col_norm == "district" or col_norm.startswith("district "):
            id_col = c
            break
    if id_col is None:
        for c in df.columns:
            if "district" in str(c).strip().lower():
                id_col = c
                break

    # Melt to long form (with or without district)
    if id_col is None:
        print("DEBUG: No District column detected; proceeding without district dimension.")
        tidy = df.melt(var_name="year_col", value_name="value")
        tidy["district"] = "ALL_ISLAND"  # optional, ensures schema parity
    else:
        value_cols = [c for c in df.columns if c != id_col]
        tidy = df.melt(id_vars=[id_col], value_vars=value_cols,
                       var_name="year_col", value_name="value")
        tidy.rename(columns={id_col: "district"}, inplace=True)

    # Remove blank row between table and annotation row
    tidy = tidy[
    tidy["district"].notna() &
    tidy["district"].astype(str).str.strip().ne("")
    ].copy()

    # Derive year label + subcategory
    tidy["year_label"], tidy["subcategory"] = zip(*tidy["year_col"].map(split_year_and_subcat))
    tidy["metric_name"]  = metric_name
    tidy["season"]       = season
    tidy["unit"]         = unit
    tidy["harvest_year"] = tidy["year_label"].map(lambda y: harvest_year_from(y, season))

    # Clean numeric values
    tidy["value"] = tidy["value"].astype(str).str.replace(",", "").str.strip()
    tidy["value"] = pd.to_numeric(tidy["value"], errors="coerce")

    # Write partitioned Parquet to Silver
    out_prefix = f"{SILVER_PREFIX}metric_name={metric_name}/season={season or 'unknown'}/"
    rows_out = 0

    for hy, sub in tidy.groupby("harvest_year", dropna=False):
        hy_str = "unknown" if pd.isna(hy) else str(int(hy))
        out_key = f"{out_prefix}harvest_year={hy_str}/part-{checksum[:10]}.parquet"

        buf = io.BytesIO()
        sub.drop(columns=["year_col"]).to_parquet(buf, index=False)
        s3.put_object(Bucket=DATA_BUCKET, Key=out_key, Body=buf.getvalue())
        rows_out += len(sub)

    return {
        "ok": True,
        "rows_out": int(rows_out),
        "metric_name": metric_name,
        "season": season,
        "unit": unit,
        "note": "Caption/header auto-detected; columns flattened; partitioned Parquet written"
    }
