"""
数据抽取模块 (Extract)
======================
Step 1: 从 CSV 文件读取订单数据，写入 ODS 层。
"""

import os
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from etl.db import get_engine

# 预期 CSV 列名与数据库列名映射
COLUMN_MAPPING = {
    "InvoiceNo": "invoice_no",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "UnitPrice": "unit_price",
    "CustomerID": "customer_id",
    "Country": "country",
}


def read_csv_data(file_path: str) -> pd.DataFrame:
    """
    读取 CSV 订单数据文件

    Args:
        file_path: CSV 文件路径

    Returns:
        pandas DataFrame
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"数据文件不存在: {file_path}")

    df = pd.read_csv(
        file_path,
        encoding="utf-8",
        dtype={
            "InvoiceNo": str,
            "StockCode": str,
            "CustomerID": str,
        },
    )
    print(f"[extract] 读取数据: {len(df)} 行, {len(df.columns)} 列")
    return df


def validate_columns(df: pd.DataFrame) -> bool:
    """
    验证 CSV 列是否完整

    Args:
        df: 原始数据 DataFrame

    Returns:
        验证通过返回 True，否则抛出 ValueError
    """
    missing = set(COLUMN_MAPPING.keys()) - set(df.columns)
    if missing:
        raise ValueError(f"CSV 缺少列: {missing}")
    print("[extract] 列校验通过 ✓")
    return True


def load_to_ods(df: pd.DataFrame) -> int:
    """
    将原始数据写入 ODS 层表 ods_orders

    Args:
        df: 原始数据 DataFrame

    Returns:
        写入行数
    """
    engine = get_engine()

    # 重命名列以匹配数据库
    df_db = df.rename(columns=COLUMN_MAPPING)

    # 转换日期格式
    df_db["invoice_date"] = pd.to_datetime(df_db["invoice_date"])

    # 清空 ODS 表后写入（全量同步模式）
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE ods_orders RESTART IDENTITY"))
        print("[extract] ODS 表已清空")

    df_db.to_sql("ods_orders", engine, if_exists="append", index=False)
    print(f"[extract] ODS 写入完成: {len(df_db)} 行")
    return len(df_db)


def run_extract(file_path: str) -> dict:
    """
    Step 1 入口：抽取 CSV → ODS

    Args:
        file_path: CSV 数据文件路径

    Returns:
        执行结果摘要
    """
    start = datetime.now()
    try:
        df = read_csv_data(file_path)
        validate_columns(df)
        row_count = load_to_ods(df)
        return {
            "task": "extract_orders",
            "status": "success",
            "rows": row_count,
            "duration": (datetime.now() - start).total_seconds(),
        }
    except Exception as e:
        return {
            "task": "extract_orders",
            "status": "failed",
            "error": str(e),
            "duration": (datetime.now() - start).total_seconds(),
        }


if __name__ == "__main__":
    import sys

    data_path = sys.argv[1] if len(sys.argv) > 1 else "data/online_retail.csv"
    result = run_extract(data_path)
    print(result)
