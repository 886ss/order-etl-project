"""
数据抽取模块 (Extract)
======================
Step 1: 从 CSV 文件读取订单数据，写入 ODS 层。
"""

import os
import logging
import pandas as pd
from datetime import datetime
from etl.db import get_engine, text, ODS_DTYPE, truncate_and_load

logger = logging.getLogger(__name__)

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

    自动尝试多种编码，兼容不同来源的 CSV 文件。
    UCI Online Retail 数据集实际编码为 Latin-1/ISO-8859-1。

    Args:
        file_path: CSV 文件路径

    Returns:
        pandas DataFrame

    Raises:
        FileNotFoundError: 文件不存在
        ValueError: 所有已知编码均无法读取
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"数据文件不存在: {file_path}")

    # cp1252 先于 latin-1：cp1252 是 latin-1 的超集，能正确解码欧元符号等字符
    # latin-1 兜底：可解码任意字节序列，永远不会抛 UnicodeDecodeError
    for enc in ["utf-8", "cp1252", "latin-1"]:
        try:
            df = pd.read_csv(
                file_path,
                encoding=enc,
                dtype={
                    "InvoiceNo": str,
                    "StockCode": str,
                    "CustomerID": str,
                },
            )
            logger.info("使用编码 %s 读取: %d 行", enc, len(df))
            return df
        except UnicodeDecodeError:
            continue

    raise ValueError(f"无法以任何已知编码读取文件: {file_path}")


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
    logger.info("列校验通过")
    return True


def load_to_ods(df: pd.DataFrame) -> int:
    """
    将原始数据写入 ODS 层表 ods_orders

    TRUNCATE 和 INSERT 在同一事务中执行，确保原子性：
    写入失败时 ODS 表不会被清空。

    Args:
        df: 原始数据 DataFrame

    Returns:
        写入行数
    """
    df_db = df.rename(columns=COLUMN_MAPPING)
    df_db["invoice_date"] = pd.to_datetime(df_db["invoice_date"])

    count = truncate_and_load("ods_orders", df_db, ODS_DTYPE)
    logger.info("ODS 写入完成: %d 行", count)
    return count


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
