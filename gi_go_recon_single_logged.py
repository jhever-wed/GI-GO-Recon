#!/usr/bin/env python3
"""
GI/GO Reconciliation — Single-File CLI with Step Checks & Logging

- Reads Atlantis (file for now; API placeholder)
- Reads GMI from file or ODBC (DB2/AS400)
- Uses embedded SQL by default (override with --sql-source file --sql-file path.sql)
- Derives the month from GMI (TEDATE) when --month is omitted
- Verifies each major step and writes a detailed log (console + file)

Dependencies:
    pip install pandas openpyxl pyodbc python-dotenv
"""
import argparse
import os
import sys
import logging
from typing import Tuple, Optional, Iterable, List, Set
from contextlib import contextmanager
from time import perf_counter
from datetime import datetime

import pandas as pd

# Optional deps
try:
    import pyodbc  # type: ignore
    PYODBC_AVAILABLE = True
except Exception:
    PYODBC_AVAILABLE = False

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# ---------------- Embedded SQL (from your gmi_query.sql) ----------------
EMBEDDED_GMI_SQL = (
    "SELECT TEDATE, TGIVIO, TEXCH, TFC, TSDSC1, TGIVF#, TOFFIC||TACCT as ACCT, "
    "TSPRED, TQTY, TFEE5,TCURSY "
    "FROM FLPGH.GMETH1F1 "
    "WHERE TRECID IN ('T','B','Q') "
    "AND TOFFIC Not IN ('CCP') "
    "AND TGIVIO IN ('GI','GO') "
    "AND TCALC <> 'R' "
    "ORDER BY TEDATE, TGIVIO, TEXCH, TGIVF#, TOFFIC, TACCT"
)

LOG = logging.getLogger("gi_go_recon")

# ---------------- Logging helpers ----------------
def setup_logging(level: str, log_file: Optional[str]) -> None:
    lvl = getattr(logging, level.upper(), logging.INFO)

    logger = logging.getLogger()
    logger.setLevel(lvl)

    # Clear any inherited handlers
    for h in list(logger.handlers):
        logger.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%Y-%m-%d %H:%M:%S")

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(lvl)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    if log_file:
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(lvl)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

def default_log_path(outdir: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(outdir, f"gi_go_recon_{ts}.log")

@contextmanager
def step(name: str):
    LOG.info("▶ %s — start", name)
    t0 = perf_counter()
    try:
        yield
    except Exception as e:
        dt = perf_counter() - t0
        LOG.exception("✖ %s — failed in %.2fs: %s", name, dt, e)
        raise
    else:
        dt = perf_counter() - t0
        LOG.info("✔ %s — done (%.2fs)", name, dt)

# ---------------- Checks ----------------
def _warn_or_raise(msg: str, strict: bool):
    if strict:
        raise RuntimeError(msg)
    else:
        LOG.warning(msg)

def check_nonempty_df(df: pd.DataFrame, label: str, strict: bool):
    if df is None or df.empty:
        _warn_or_raise(f"{label} is empty.", strict)
    else:
        LOG.info("%s rows: %,d | cols: %s", label, len(df), list(df.columns))

def check_has_columns(df: pd.DataFrame, required: Iterable[str], label: str, strict: bool):
    req = set(required)
    missing = [c for c in req if c not in df.columns]
    if missing:
        _warn_or_raise(f"{label} missing columns: {missing}", strict)
    else:
        LOG.info("%s has required columns.", label)

# ---------------- Loaders ----------------
def _read_any(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        return pd.read_csv(path, low_memory=False)
    elif ext in (".xlsx", ".xls"):
        return pd.read_excel(path)
    else:
        try:
            return pd.read_csv(path, low_memory=False)
        except Exception:
            return pd.read_excel(path)

def load_atlantis(atlantis_source: str, atlantis_file: Optional[str]) -> pd.DataFrame:
    if atlantis_source == "file":
        if not atlantis_file or not os.path.exists(atlantis_file):
            raise FileNotFoundError(f"Atlantis file not found: {atlantis_file}")
        df = _read_any(atlantis_file)
        return df
    elif atlantis_source == "api":
        raise NotImplementedError("Atlantis API source not yet implemented. Use --atlantis-source file for now.")
    else:
        raise ValueError("Invalid atlantis_source. Use 'file' or 'api'.")

def fetch_gmi_via_odbc(driver: str, system: str, uid: str, pwd: str, sql_text: str) -> pd.DataFrame:
    if not PYODBC_AVAILABLE:
        raise RuntimeError("pyodbc is not installed. Install with: pip install pyodbc")
    conn_str = f"DRIVER={{{driver}}};SYSTEM={system};UID={uid};PWD={pwd};"
    LOG.info("Connecting ODBC driver=%s system=%s uid=%s", driver, system, uid)
    cnxn = pyodbc.connect(conn_str, autocommit=True, timeout=60)
    try:
        df = pd.read_sql_query(sql_text, cnxn)
    finally:
        cnxn.close()
    return df

def load_gmi(gmi_source: str, gmi_file: Optional[str],
             driver: Optional[str], system: Optional[str], uid: Optional[str],
             pwd: Optional[str], sql_source: str, sql_file: Optional[str]) -> pd.DataFrame:
    if gmi_source == "file":
        if not gmi_file or not os.path.exists(gmi_file):
            raise FileNotFoundError(f"GMI file not found: {gmi_file}")
        df = _read_any(gmi_file)
        return df
    elif gmi_source == "odbc":
        if not all([driver, system, uid]):
            raise ValueError("ODBC mode requires --odbc-driver, --odbc-system, --odbc-uid (password via --odbc-pwd or env).")
        if sql_source == "embedded":
            sql_text = EMBEDDED_GMI_SQL
        else:
            if not sql_file or not os.path.exists(sql_file):
                raise FileNotFoundError(f"SQL file not found: {sql_file}")
            with open(sql_file, "r", encoding="utf-8") as f:
                sql_text = f.read()
        if not pwd:
            pwd = os.getenv("GMI_PWD", "")
        if not pwd:
            raise ValueError("No ODBC password provided. Use --odbc-pwd or env var GMI_PWD.")
        return fetch_gmi_via_odbc(driver, system, uid, pwd, sql_text)
    else:
        raise ValueError("Invalid gmi_source. Use 'file' or 'odbc'.")

# ---------------- Transforms ----------------
def prep_frames(df1_raw: pd.DataFrame, df2_raw: pd.DataFrame, mode: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df1 = df1_raw.copy()
    df2 = df2_raw.copy()

    # Clean headers
    df1.columns = df1.columns.str.strip()
    df2.columns = df2.columns.str.strip()

    # Filters
    if "RecordType" in df1.columns:
        df1 = df1[df1["RecordType"] == "TP"]
    if "TGIVIO" in df2.columns:
        df2 = df2[df2["TGIVIO"] == mode]

    # Column mapping — Atlantis
    atlantis_common = {
        "TradeDate": "Date",
        "Quantity": "Qty",
        "Product": "SYM",
        "GiveUpAmt": "Fee",
        "ClearingAccount": "Account",
    }
    cb_col_pref = "ExchangeEBCode" if mode == "GI" else "ExchangeCBCode"
    cb_col = cb_col_pref if cb_col_pref in df1.columns else (
        "ExchangeEBCode" if "ExchangeEBCode" in df1.columns else (
            "ExchangeCBCode" if "ExchangeCBCode" in df1.columns else None
        )
    )
    atlantis_map = atlantis_common.copy()
    if cb_col is not None:
        atlantis_map[cb_col] = "CB"
    df1 = df1.rename(columns=atlantis_map)

    # Column mapping — GMI
    gmi_map = {
        "TGIVF#": "CB",
        "TEDATE": "Date",
        "TQTY": "Qty",
        "TFC": "SYM",
        "TFEE5": "Fee",
        "ACCT": "Account",
        "Acct": "Account",
    }
    df2 = df2.rename(columns=gmi_map)

    # Dates
    if "Date" in df1.columns:
        df1["Date"] = pd.to_datetime(df1["Date"].astype(str), format="%Y%m%d", errors="coerce")
    if "Date" in df2.columns:
        try:
            df2["Date"] = pd.to_datetime(df2["Date"])
        except Exception:
            df2["Date"] = pd.to_datetime(df2["Date"].astype(str), format="%Y%m%d", errors="coerce")

    # Numerics
    for col in ["Qty", "Fee"]:
        if col in df1.columns:
            df1[col] = pd.to_numeric(df1[col], errors="coerce")
        if col in df2.columns:
            df2[col] = pd.to_numeric(df2[col], errors="coerce")

    # Trim CB
    if "CB" in df1.columns:
        df1["CB"] = df1["CB"].astype(str).str.strip()
    if "CB" in df2.columns:
        df2["CB"] = df2["CB"].astype(str).str.strip()

    return df1, df2

def summarize_and_merge(df1: pd.DataFrame, df2: pd.DataFrame):
    group_keys = ["CB", "Date", "Account", "SYM"]
    if set(group_keys).issubset(df1.columns):
        summary1 = df1.groupby(group_keys, dropna=False)[["Qty", "Fee"]].sum().reset_index()
        summary1 = summary1.rename(columns={"Qty": "Qty_Atlantis", "Fee": "Fee_Atlantis"})
    else:
        summary1 = pd.DataFrame(columns=group_keys + ["Qty_Atlantis", "Fee_Atlantis"])

    if set(group_keys).issubset(df2.columns):
        summary2 = df2.groupby(group_keys, dropna=False)[["Qty", "Fee"]].sum().reset_index()
        summary2 = summary2.rename(columns={"Qty": "Qty_GMI", "Fee": "Fee_GMI"})
    else:
        summary2 = pd.DataFrame(columns=group_keys + ["Qty_GMI", "Fee_GMI"])

    merged = pd.merge(summary1, summary2, on=group_keys, how="outer")
    for col in ["Qty_Atlantis", "Fee_Atlantis", "Qty_GMI", "Fee_GMI"]:
        merged[col] = merged[col].fillna(0)
    merged["Qty_Diff"] = (merged["Qty_Atlantis"] - merged["Qty_GMI"]).round(2)
    merged["Fee_Diff"] = (merged["Fee_Atlantis"] + merged["Fee_GMI"]).round(2)

    present_cols = [c for c in ["Qty_Atlantis", "Fee_Atlantis", "Qty_GMI", "Fee_GMI"] if c in merged.columns]
    if "CB" in merged.columns and present_cols:
        top_summary = merged.groupby("CB")[present_cols].sum().reset_index()
        top_summary["Qty_Diff"] = (top_summary["Qty_Atlantis"] - top_summary["Qty_GMI"]).round(2)
        top_summary["Fee_Diff"] = (top_summary["Fee_Atlantis"] + top_summary["Fee_GMI"]).round(2)
    else:
        top_summary = pd.DataFrame()

    matched = merged[(merged["Qty_Diff"] == 0) & (merged["Fee_Diff"] == 0)]
    qty_match_only = merged[(merged["Qty_Diff"] == 0) & (merged["Fee_Diff"] != 0)]
    fee_match_only = merged[(merged["Qty_Diff"] != 0) & (merged["Fee_Diff"] == 0)]
    no_match = merged[(merged["Qty_Diff"] != 0) & (merged["Fee_Diff"] != 0)]
    return top_summary, matched, qty_match_only, fee_match_only, no_match

def month_detect_or_filter(df1: pd.DataFrame, df2: pd.DataFrame, month: Optional[str]):
    """
    Month selection logic:
      - If --month provided, use it.
      - Else, derive the month from **GMI** ('TEDATE' -> mapped to 'Date').
      - If GMI has multiple months, pick the latest and WARN.
      - If GMI has no dates, fall back to auto-detect latest across both sides.
    """
    if month:
        try:
            period = pd.Period(month, freq="M")
        except Exception as e:
            raise ValueError(f"Invalid --month '{month}'. Expected YYYY-MM.") from e
    else:
        # Prefer GMI months
        months2 = []
        if "Date" in df2.columns:
            try:
                months2 = df2["Date"].dt.to_period("M").dropna().unique()
            except Exception:
                months2 = []
        if len(months2) == 1:
            period = months2[0]
            month = period.strftime("%Y-%m")
            LOG.info("Using month from GMI: %s", month)
        elif len(months2) > 1:
            all_months_sorted = sorted(set(months2), reverse=True)
            period = all_months_sorted[0]
            month = period.strftime("%Y-%m")
            LOG.warning("Multiple months detected in GMI: %s. Using latest: %s",
                        ", ".join(sorted({m.strftime('%Y-%m') for m in months2})), month)
        else:
            # Fallback to combined detection
            months1 = df1["Date"].dt.to_period("M").dropna().unique() if "Date" in df1.columns else []
            months2 = df2["Date"].dt.to_period("M").dropna().unique() if "Date" in df2.columns else []
            all_months = sorted(set(months1).union(set(months2)), reverse=True)
            if not all_months:
                raise ValueError("Could not determine month from GMI. Provide --month YYYY-MM.")
            period = all_months[0]
            month = period.strftime("%Y-%m")
            LOG.info("Fallback: auto-detected latest month across both: %s", month)

    if "Date" in df1.columns:
        df1 = df1[df1["Date"].dt.to_period("M") == period]
    if "Date" in df2.columns:
        df2 = df2[df2["Date"].dt.to_period("M") == period]
    return df1, df2, month or period.strftime("%Y-%m")

def write_excel(outdir: str, mode: str, month: str,
                top_summary: pd.DataFrame, matched: pd.DataFrame,
                qty_match_only: pd.DataFrame, fee_match_only: pd.DataFrame, no_match: pd.DataFrame) -> str:
    # Lazy import to give better error if missing
    try:
        import openpyxl  # type: ignore
    except Exception as e:
        raise RuntimeError("Missing dependency 'openpyxl'. Install with: pip install openpyxl") from e

    os.makedirs(outdir, exist_ok=True)
    fname = os.path.join(outdir, f"reconciliation_{mode}_{month}.xlsx")
    with pd.ExcelWriter(fname, engine="openpyxl") as writer:
        if not top_summary.empty:
            top_summary.to_excel(writer, sheet_name="Top Summary by CB", index=False)
        matched.to_excel(writer, sheet_name="Full Matches", index=False)
        qty_match_only.to_excel(writer, sheet_name="Qty Match Only", index=False)
        fee_match_only.to_excel(writer, sheet_name="Fee Match Only", index=False)
        no_match.to_excel(writer, sheet_name="No Match", index=False)
    if not os.path.exists(fname) or os.path.getsize(fname) == 0:
        raise IOError(f"Failed to write Excel: {fname}")
    return fname

# ---------------- CLI ----------------
def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="GI/GO reconciliation (single-file, with step checks & logging)")
    # Atlantis
    p.add_argument("--atlantis-source", choices=["file", "api"], default="file",
                   help="How to load Atlantis data (default: file)")
    p.add_argument("--atlantis-file", help="Path to Atlantis CSV/XLS(X) when --atlantis-source=file")

    # GMI
    p.add_argument("--gmi-source", choices=["file", "odbc"], default="file",
                   help="How to load GMI data (default: file)")
    p.add_argument("--gmi-file", help="Path to GMI CSV/XLS(X) when --gmi-source=file")
    p.add_argument("--odbc-driver", help="ODBC driver name (e.g., 'Client Access ODBC Driver (32-bit)' or 'IBM i Access ODBC Driver')")
    p.add_argument("--odbc-system", help="Host/IP for the IBM i system")
    p.add_argument("--odbc-uid", help="User ID for ODBC")
    p.add_argument("--odbc-pwd", help="Password (or set env GMI_PWD)")
    p.add_argument("--sql-source", choices=["embedded", "file"], default="embedded",
                   help="Use embedded SQL (default) or read from --sql-file")
    p.add_argument("--sql-file", help="Path to .sql when --sql-source=file")

    # Run options
    p.add_argument("--report", choices=["GI", "GO", "BOTH"], default="BOTH", help="Which report to generate (default: BOTH)")
    p.add_argument("--month", help="Month filter YYYY-MM. If omitted, derived from GMI's TEDATE.")
    p.add_argument("--outdir", default=r"P:\RECON", help="Output directory for Excel files (default: P:\\RECON)")

    # Logging/validation
    p.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    p.add_argument("--log-file", help="Path to log file (default: <outdir>/gi_go_recon_<timestamp>.log)")
    p.add_argument("--strict", action="store_true",
                   help="Treat missing columns/empty steps as errors (default: warnings)")

    # Utility
    p.add_argument("--print-sql", action="store_true", help="Print the embedded SQL and exit")
    return p.parse_args(argv)

def run_one_mode(atlantis_df: pd.DataFrame, gmi_df: pd.DataFrame, mode: str, month: Optional[str], outdir: str, strict: bool) -> str:
    with step(f"{mode} | prep frames & filters"):
        f1, f2 = prep_frames(atlantis_df, gmi_df, mode)
        check_nonempty_df(f1, f"{mode} | Atlantis after filter", strict)
        check_nonempty_df(f2, f"{mode} | GMI after filter", strict)

        # Expect mapped columns for downstream grouping
        check_has_columns(f1, ["Date"], f"{mode} | Atlantis mapped", strict)
        check_has_columns(f2, ["Date", "CB"], f"{mode} | GMI mapped", strict)

    with step(f"{mode} | month selection & filter"):
        f1, f2, month_used = month_detect_or_filter(f1, f2, month)
        check_nonempty_df(f1, f"{mode} | Atlantis after month", strict)
        check_nonempty_df(f2, f"{mode} | GMI after month", strict)

    with step(f"{mode} | summarize & merge"):
        top_summary, matched, qty_match_only, fee_match_only, no_match = summarize_and_merge(f1, f2)
        LOG.info("%s | counts — top_summary=%d matched=%d qty_only=%d fee_only=%d no_match=%d",
                 mode, len(top_summary), len(matched), len(qty_match_only), len(fee_match_only), len(no_match))

    with step(f"{mode} | write Excel"):
        out = write_excel(outdir, mode, month_used, top_summary, matched, qty_match_only, fee_match_only, no_match)
        size_kb = os.path.getsize(out) / 1024.0
        LOG.info("%s | wrote: %s (%.1f KB)", mode, out, size_kb)

    return out

def main(argv=None) -> int:
    args = parse_args(argv)

    # Prepare log file path
    log_file = args.log_file or default_log_path(args.outdir)
    setup_logging(args.log_level, log_file)
    LOG.info("Logs will be written to: %s", log_file)

    if args.print_sql:
        print(EMBEDDED_GMI_SQL)
        return 0

    try:
        with step("load Atlantis"):
            atlantis_df = load_atlantis(args.atlantis_source, args.atlantis_file)
            check_nonempty_df(atlantis_df, "Atlantis raw", args.strict)
            # Light sanity check for expected raw columns (warn if missing)
            expected_atlantis = ["TradeDate", "Quantity", "Product", "GiveUpAmt"]
            check_has_columns(atlantis_df, expected_atlantis, "Atlantis raw (recommended cols)", False)

        with step("load GMI"):
            gmi_df = load_gmi(args.gmi_source, args.gmi_file, args.odbc_driver, args.odbc_system,
                              args.odbc_uid, args.odbc_pwd, args.sql_source, args.sql_file)
            check_nonempty_df(gmi_df, "GMI raw", args.strict)
            expected_gmi = ["TEDATE", "TGIVIO", "TGIVF#", "TQTY", "TFEE5"]
            check_has_columns(gmi_df, expected_gmi, "GMI raw (recommended cols)", False)

        outputs = []
        if args.report in ("GI", "BOTH"):
            outputs.append(run_one_mode(atlantis_df, gmi_df, "GI", args.month, args.outdir, args.strict))
        if args.report in ("GO", "BOTH"):
            outputs.append(run_one_mode(atlantis_df, gmi_df, "GO", args.month, args.outdir, args.strict))

        # Verify outputs
        with step("verify outputs"):
            bad = [p for p in outputs if (not os.path.exists(p) or os.path.getsize(p) == 0)]
            if bad:
                LOG.error("Some outputs failed to write: %s", ", ".join(bad))
                return 4
            LOG.info("All outputs verified OK: %s", ", ".join(outputs))

    except Exception as e:
        LOG.error("Run aborted due to error.")
        return 3

    LOG.info("All done.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
