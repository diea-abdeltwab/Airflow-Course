# ===================== Import Libraries =====================
import pandas as pd
import os

# ===================== Constants =====================
FILE_PATH       = '/opt/airflow/dags/data/cleaned'
SQL_OUTPUT_PATH = '/opt/airflow/dags/data/sql'

# ===================== SQL: Create Tables =====================
CREATE_SALES_SQL = """
DROP TABLE IF EXISTS sales;
CREATE TABLE sales (
    order_id      INT,
    date          DATE,
    customer_name VARCHAR,
    product       VARCHAR,
    category      VARCHAR,
    quantity      INT,
    unit_price    FLOAT,
    total_price   FLOAT,
    region        VARCHAR,
    status        VARCHAR,
    cleaned_on    DATE,
    source_file   VARCHAR
);
"""

CREATE_ORDERS_SQL = """
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id         INT,
    customer_id      VARCHAR,
    customer_name    VARCHAR,
    order_date       DATE,
    product          VARCHAR,
    category         VARCHAR,
    quantity         INT,
    unit_price       FLOAT,
    total_price      FLOAT,
    shipping_address VARCHAR,
    city             VARCHAR,
    status           VARCHAR,
    payment_method   VARCHAR,
    cleaned_on       DATE,
    source_file      VARCHAR
);
"""

CREATE_RETURNS_SQL = """
DROP TABLE IF EXISTS returns;
CREATE TABLE returns (
    return_id         VARCHAR,
    order_id          INT,
    customer_name     VARCHAR,
    return_date       DATE,
    product           VARCHAR,
    category          VARCHAR,
    quantity_returned  INT,
    unit_price        FLOAT,
    refund_amount     FLOAT,
    return_reason     VARCHAR,
    status            VARCHAR,
    refund_method     VARCHAR,
    cleaned_on        DATE,
    source_file       VARCHAR
);
"""

# ===================== Helper: CSV -> SQL =====================
def csv_to_sql(table_name, file_name, **context):
    ds_nodash = context['ds_nodash']
    csv_file  = f"{FILE_PATH}/{file_name}_cleaned_{ds_nodash}.csv"
    sql_file  = f"{SQL_OUTPUT_PATH}/insert_{file_name}_cleaned.sql"

    if not os.path.exists(csv_file):
        raise FileNotFoundError(
            f"CSV file not found: {csv_file}\n"
            f"Make sure 'data_cleaning_pipeline' ran successfully on the same date ({ds_nodash})."
        )

    df = pd.read_csv(csv_file)

    # Convert cleaned_on from integer (20260504) to DATE string ('2026-05-04')
    if 'cleaned_on' in df.columns:
        df['cleaned_on'] = pd.to_datetime(
            df['cleaned_on'].astype(str), format='%Y%m%d'
        ).dt.strftime('%Y-%m-%d')

    columns = list(df.columns)

    def format_value(v):
        if pd.isna(v):
            return "NULL"
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"
        return str(v)

    rows = []
    for _, row in df.iterrows():
        values = ", ".join(format_value(row[col]) for col in columns)
        rows.append(f"({values})")

    sql = (
        f"-- Insert {table_name}\n"
        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES\n"
        + ",\n".join(rows) + ";"
    )

    with open(sql_file, "w", encoding="utf-8") as f:
        f.write(sql)

    print(f"Generated : {sql_file}  ({len(rows)} rows)")
