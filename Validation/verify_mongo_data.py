#!/usr/bin/env python3
"""
Post-process & validate AEP extracted data

Usage:
  - Ensure energy.env is in the same folder containing:
      MONGO_URI=...
      DB_NAME=africa_energy
      RAW_COLLECTION=energy_indicators         # where raw extracted data is stored (optional)
      FINAL_COLLECTION=energy_indicators_final # where normalized docs will be stored
  - If you have a backup CSV produced by the extractor named 'aep_parsed_backup.csv',
    this script will prefer reading that. Otherwise it will pull all docs from RAW_COLLECTION.

  - Run:
      pip install pandas pymongo python-dotenv tqdm
      python aep_postprocess_validate.py
"""

import os
import json
import math
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, ReplaceOne
from tqdm import tqdm

# -------- CONFIG ----------
load_dotenv("energy.env")

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "africa_energy")
RAW_COLLECTION = os.getenv("RAW_COLLECTION", "energy_indicators")          # where raw extractor stored raw rows
FINAL_COLLECTION = os.getenv("FINAL_COLLECTION", "energy_indicators_final")  # normalized collection
BACKUP_CSV = Path.cwd() / "aep_parsed_backup.csv"  # prefer this if exists

YEARS = [str(y) for y in range(2000, 2025)]
REQUIRED_SCHEMA = ["country", "country_serial", "metric", "unit", "sector", "sub_sector",
                   "sub_sub_sector", "source_link", "source"] + YEARS

# A simple unit normalization map (extend as needed)
UNIT_MAP = {
    "%": ["%", "percent", "percentage"],
    "GWh": ["gwh", "giga watt hour", "gwh "],
    "MW": ["mw", "megawatt"],
    "kWh per capita": ["kwh per capita", "kwh/capita", "kwh per person"],
    "kt": ["kt", "kilotonnes", "kt "]
}

# -------- HELPERS ----------
def connect_mongo():
    if not MONGO_URI:
        raise RuntimeError("MONGO_URI missing in energy.env")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return client, db

def load_raw_dataframe() -> pd.DataFrame:
    """
    Load raw parsed rows, preferring local backup CSV if present,
    otherwise reading from RAW_COLLECTION in MongoDB.
    """
    if BACKUP_CSV.exists():
        print(f"🔎 Loading parsed backup CSV: {BACKUP_CSV}")
        df = pd.read_csv(BACKUP_CSV, dtype=str)
        df.fillna("", inplace=True)
        return df
    else:
        print("🔌 No local backup CSV found — pulling raw docs from MongoDB raw collection...")
        client, db = connect_mongo()
        raw_coll = db[RAW_COLLECTION]
        docs = list(raw_coll.find({}))
        client.close()
        if not docs:
            print("⚠️ No raw docs found in MongoDB. Aborting.")
            return pd.DataFrame()
        df = pd.DataFrame(docs)
        df = df.where(pd.notnull(df), None)
        return df

def normalize_unit(unit_raw: Optional[str]) -> Optional[str]:
    """Normalize unit string to a canonical unit if possible."""
    if not unit_raw:
        return None
    s = str(unit_raw).strip().lower()
    if s == "":
        return None
    for canon, variants in UNIT_MAP.items():
        for v in variants:
            if v in s:
                return canon
    # some common symbols
    if s in ["%", "percent", "percentage"]:
        return "%"
    return unit_raw.strip()

def to_number_safe(x) -> Optional[float]:
    """Convert a raw extracted value to float, or None if not convertible."""
    if x is None:
        return None
    if isinstance(x, (int, float)) and not math.isnan(x):
        return float(x)
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "n/a", "na"):
        return None
    # remove commas and footnotes
    s = s.replace(",", "")
    s = s.split()[0]  # in case "123 (est)"
    try:
        return float(s)
    except:
        # remove any non-numeric characters
        import re
        m = re.search(r"[-+]?\d*\.\d+|\d+", s)
        if m:
            try:
                return float(m.group(0))
            except:
                return None
        return None

def infer_country_serial(country_name: Optional[str]) -> Optional[int]:
    if not country_name:
        return None
    return abs(hash(country_name)) % 100000  # stable-ish serial

# -------- TRANSFORM ----------
def build_normalized_docs(df_raw: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Convert raw dataframe rows into normalized documents according to REQUIRED_SCHEMA.
    The raw input can be messy: columns vary. Strategy:
      - If row already contains 'metric' and year columns, use those.
      - If row has wide format with year columns, build one doc per row (metric per country).
      - If row has long format (Year, Value), pivot before calling this function (not implemented here).
    """
    docs = []

    # unify column names to lower for detection while preserving originals
    col_map = {c: c for c in df_raw.columns}
    lc_map = {c.lower(): c for c in df_raw.columns}

    # detection heuristics
    # possible metric column names
    metric_candidates = [k for k in lc_map.keys() if any(x in k for x in ["metric","indicator","series","name","variable"])]
    unit_candidates = [k for k in lc_map.keys() if "unit" in k or "measure" in k]

    country_candidates = [k for k in lc_map.keys() if "country" in k]

    # If the df has any of the YEAR columns as columns, treat it as wide table
    has_year_cols = any(y in df_raw.columns for y in YEARS)

    for idx, row in df_raw.iterrows():
        # try to derive base fields
        country = None
        if country_candidates:
            country = row[lc_map[country_candidates[0]]] if lc_map[country_candidates[0]] in row else None
        else:
            # maybe there is a 'country' key inside a 'source' or 'source_link' - skip
            country = row.get("country") if "country" in row else None

        # standardize
        country = str(country).strip() if country and country != "None" else None

        metric = None
        for m in metric_candidates:
            if lc_map.get(m):
                metric = row[lc_map[m]]
                break
        if not metric and "metric" in row:
            metric = row.get("metric")
        if not metric and len(df_raw.columns) > 0:
            # fallback: first col that's not a year
            for c in df_raw.columns:
                if c in YEARS:
                    continue
                # skip obvious metadata
                if c.lower() in ("source","source_link","unit","country","country_serial"):
                    continue
                metric = row.get(c)
                if metric and str(metric).strip():
                    break

        metric = str(metric).strip() if metric and metric != "None" else None

        unit_raw = None
        if unit_candidates:
            unit_raw = row[lc_map[unit_candidates[0]]] if lc_map[unit_candidates[0]] in row else None
        elif "unit" in row:
            unit_raw = row.get("unit")
        unit = normalize_unit(unit_raw)

        # base doc
        base = {
            "country": country,
            "country_serial": infer_country_serial(country),
            "metric": metric,
            "unit": unit,
            "sector": row.get("sector") if "sector" in row else None,
            "sub_sector": row.get("sub_sector") if "sub_sector" in row else None,
            "sub_sub_sector": row.get("sub_sub_sector") if "sub_sub_sector" in row else None,
            "source_link": row.get("source_link") if "source_link" in row else row.get("source_link"),
            "source": row.get("source") if "source" in row else "Africa Energy Portal"
        }

        # ensure years
        year_values = {}
        if has_year_cols:
            for y in YEARS:
                if y in df_raw.columns:
                    v = row.get(y)
                    year_values[y] = to_number_safe(v)
                else:
                    # try columns with year nested, e.g., "2000 (GWh)"
                    found = None
                    for c in df_raw.columns:
                        if y in str(c):
                            found = to_number_safe(row.get(c))
                            break
                    year_values[y] = found
        else:
            # no year columns present — maybe row is single-year or long-form; leave years None
            for y in YEARS:
                year_values[y] = None

        # assemble final document
        doc = dict(base)
        doc.update(year_values)

        # Only keep doc if metric present
        if doc["metric"] and doc["metric"].strip():
            docs.append(doc)

    return docs

# -------- UPSERT ----------
def upsert_normalized_docs(coll, docs: List[Dict[str, Any]]) -> Tuple[int,int]:
    """
    Upsert docs into final collection using (country, metric) as key when country exists,
    otherwise use (metric, source_link).
    Returns (upserted_count, modified_count).
    """
    ops = []
    for d in docs:
        # ensure schema keys exist
        for k in REQUIRED_SCHEMA:
            d.setdefault(k, None)
        # create match key
        if d.get("country"):
            key = {"country": d["country"], "metric": d["metric"]}
        else:
            key = {"metric": d["metric"], "source_link": d.get("source_link")}
        ops.append(ReplaceOne(key, d, upsert=True))
    if not ops:
        return 0,0
    res = coll.bulk_write(ops, ordered=False)
    return getattr(res, "upserted_count", 0), getattr(res, "modified_count", 0)

# -------- VALIDATION REPORT ----------
def validate_collection(coll) -> Dict[str, Any]:
    """
    Validate final collection:
      - missing years per doc,
      - inconsistent units per metric,
      - country coverage (unique countries),
      - completeness % per year.
    """
    report = {}
    docs = list(coll.find({}))
    if not docs:
        return {"error":"no documents found in final collection"}

    total_docs = len(docs)
    report["total_documents"] = total_docs

    # missing years
    missing_years = {}
    for d in docs:
        missing = [y for y in YEARS if d.get(y) is None]
        if missing:
            key = f"{d.get('country')}||{d.get('metric')}"
            missing_years[key] = missing
    report["missing_years"] = missing_years

    # inconsistent units
    metric_units = {}
    for d in docs:
        m = d.get("metric")
        u = d.get("unit")
        metric_units.setdefault(m, set())
        if u:
            metric_units[m].add(u)
    inconsistent = {m: list(units) for m,units in metric_units.items() if len(units) > 1}
    report["inconsistent_units"] = inconsistent

    # country coverage
    countries = sorted([c for c in coll.distinct("country") if c])
    report["countries_count"] = len(countries)
    report["countries"] = countries

    # completeness per year
    completeness = {}
    for y in YEARS:
        non_null = coll.count_documents({y: {"$ne": None}})
        completeness[y] = {"non_null": non_null, "percent": round(non_null/total_docs*100,1)}
    report["completeness_by_year"] = completeness

    return report

# ---------- MAIN ----------
def main():
    print("🔁 AEP post-processing and validation started")
    df_raw = load_raw_dataframe()
    if df_raw.empty:
        print("❌ No data to process. Exiting.")
        return

    print(f"ℹ️ Raw rows loaded: {len(df_raw)}")
    normalized_docs = build_normalized_docs(df_raw)
    print(f"✅ Normalized documents prepared: {len(normalized_docs)}")

    client, db = connect_mongo()
    final_coll = db[FINAL_COLLECTION]

    u, m = upsert_normalized_docs(final_coll, normalized_docs)
    print(f"💾 Upserted into final collection: upserted={u}, modified={m}")

    # backup CSV of final docs
    final_docs = list(final_coll.find({}))
    if final_docs:
        df_final = pd.DataFrame(final_docs)
        backup_csv = Path.cwd() / "energy_indicators_final_backup.csv"
        df_final.to_csv(backup_csv, index=False)
        print(f"💾 Final backup CSV saved: {backup_csv}")

    # validation
    report = validate_collection(final_coll)
    report_path = Path.cwd() / "aep_validation_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"📄 Validation report saved to {report_path}")

    print("✅ Post-processing & validation completed.")
    client.close()

if __name__ == "__main__":
    main()
