"""
cleandata.py — Data Cleaning Script for Airflow Pipeline
Usage: python cleandata.py <run_date> <input_file>
"""

import sys
import os
import pandas as pd

# ══════════════════════════════════════════════════════════════
# 0. Helpers
# ══════════════════════════════════════════════════════════════

def validate_columns(df, required_cols, file_type):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"[{file_type}] Missing expected columns: {missing}\n"
            f"  Found columns: {list(df.columns)}"
        )


def drop_missing(df, cols):
    return df.dropna(subset=cols)


def fix_missing_amount(df, target, qty_col, price_col):
    mask = df[target].isna()
    df.loc[mask, target] = df.loc[mask, qty_col] * df.loc[mask, price_col]


def drop_invalid_qty(df, qty_col):
    return df[df[qty_col] > 0]


def drop_outliers_iqr(df, col):
    Q1, Q3 = df[col].quantile(0.25), df[col].quantile(0.75)
    IQR = Q3 - Q1
    mask = (df[col] < Q1 - 1.5 * IQR) | (df[col] > Q3 + 1.5 * IQR)
    return df[~mask]


def normalize_text(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()


def parse_dates(df, date_col):
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])
    df[date_col] = df[date_col].dt.strftime("%Y-%m-%d")
    return df


# ══════════════════════════════════════════════════════════════
# 1. File-specific cleaners
# ══════════════════════════════════════════════════════════════

REQUIRED_COLS = {
    "sales": [
        "order_id", "date", "customer_name", "product", "category",
        "quantity", "unit_price", "total_price", "region", "status"
    ],
    "orders": [
        "order_id", "customer_id", "customer_name", "order_date", "product",
        "category", "quantity", "unit_price", "total_price",
        "shipping_address", "city", "status", "payment_method"
    ],
    "returns": [
        "return_id", "order_id", "customer_name", "return_date", "product",
        "category", "quantity_returned", "unit_price", "refund_amount",
        "return_reason", "status", "refund_method"
    ],
}


def clean_sales(df):
    validate_columns(df, REQUIRED_COLS["sales"], "sales")
    df = drop_missing(df, ["customer_name"])
    fix_missing_amount(df, "total_price", "quantity", "unit_price")
    df = drop_invalid_qty(df, "quantity")
    df = drop_outliers_iqr(df, "total_price")
    normalize_text(df, ["customer_name", "category", "region", "status"])
    df = parse_dates(df, "date")
    return df


def clean_orders(df):
    validate_columns(df, REQUIRED_COLS["orders"], "orders")
    df = drop_missing(df, ["customer_name"])
    fix_missing_amount(df, "total_price", "quantity", "unit_price")
    df = drop_invalid_qty(df, "quantity")
    df = drop_outliers_iqr(df, "total_price")
    normalize_text(df, ["customer_name", "category", "city", "status", "payment_method"])
    df = parse_dates(df, "order_date")
    return df


def clean_returns(df):
    validate_columns(df, REQUIRED_COLS["returns"], "returns")
    df = drop_missing(df, ["customer_name", "product"])
    fix_missing_amount(df, "refund_amount", "quantity_returned", "unit_price")
    df = drop_invalid_qty(df, "quantity_returned")
    df = drop_outliers_iqr(df, "refund_amount")
    normalize_text(df, ["customer_name", "category", "return_reason", "status", "refund_method"])
    df = parse_dates(df, "return_date")
    return df


# ══════════════════════════════════════════════════════════════
# 2. Router
# ══════════════════════════════════════════════════════════════

CLEANERS = {
    "sales":   clean_sales,
    "orders":  clean_orders,
    "returns": clean_returns,
}


def get_cleaner(filename):
    stem = os.path.splitext(os.path.basename(filename))[0].lower()
    for key, fn in CLEANERS.items():
        if key in stem:
            return key, fn
    raise ValueError(
        f"No cleaner found for '{filename}'. "
        f"Supported types: {list(CLEANERS.keys())}"
    )


# ══════════════════════════════════════════════════════════════
# 3. Entry point
# ══════════════════════════════════════════════════════════════

def main():
    if len(sys.argv) < 3:
        print("Usage: python cleandata.py <run_date> <input_file>")
        sys.exit(1)

    run_date   = sys.argv[1]
    input_file = sys.argv[2]
    output_dir = sys.argv[3]
	
    df = pd.read_csv(input_file)

    file_type, cleaner_fn = get_cleaner(input_file)

    try:
        df = cleaner_fn(df)
    except Exception as e:
        print(f"ERROR during cleaning: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    df["cleaned_on"]  = run_date
    df["source_file"] = os.path.basename(input_file)

    os.makedirs(output_dir, exist_ok=True)
    stem        = os.path.splitext(os.path.basename(input_file))[0]
    output_file = os.path.join(output_dir, f"{stem}_cleaned_{run_date}.csv")
    df.to_csv(output_file, index=False)


if __name__ == "__main__":
    main()
