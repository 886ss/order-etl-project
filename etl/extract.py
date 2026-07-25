"""
数据抽取模块 (Extract)
======================
Step 1: 从 CSV 文件读取数据，写入 ODS 层。

双模式:
  - run_extract():    YAML schema 驱动（UCI 兼容，向后兼容）
  - auto_extract():   自动推断 schema（任意 CSV，零配置）
"""

import os
import logging
import pandas as pd
from datetime import datetime
from etl.db import get_engine, text, ODS_DTYPE, truncate_and_load, append_to_table, upsert_incremental, dynamic_load
from etl.sanitizer import read_csv_safe
from etl.profiler import profile_dataframe, infer_and_clean_dates
from etl.config import (
    RuntimeSchema, load_yaml_schema, yaml_to_runtime_schema,
    merge_schemas, save_cache,
)

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


def load_to_ods(df: pd.DataFrame, incremental: bool = False) -> int:
    """
    将原始数据写入 ODS 层表 ods_orders

    全量模式（默认）：TRUNCATE + INSERT 同事务，写入失败自动回滚。
    增量模式：幂等追加（DELETE 日期批次 + INSERT），Airflow 重跑不重复。

    Args:
        df: 原始数据 DataFrame
        incremental: True 时使用追加模式（不清空 ODS）

    Returns:
        写入行数
    """
    df_db = df.rename(columns=COLUMN_MAPPING)
    df_db["invoice_date"] = pd.to_datetime(df_db["invoice_date"])

    if incremental:
        count = upsert_incremental("ods_orders", df_db, ODS_DTYPE, "invoice_date")
        logger.info("ODS 增量写入（幂等）: %d 行", count)
    else:
        count = truncate_and_load("ods_orders", df_db, ODS_DTYPE)
        logger.info("ODS 全量写入: %d 行", count)
    return count


def run_extract(file_path: str, incremental: bool = False, since_date: str = None) -> dict:
    """
    Step 1 入口：抽取 CSV → ODS

    Args:
        file_path: CSV 数据文件路径
        incremental: True 时仅抽取 since_date 之后的新数据，追加到 ODS
        since_date: 增量抽取的起始日期（YYYY-MM-DD），仅在 incremental=True 时生效

    Returns:
        执行结果摘要
    """
    start = datetime.now()
    try:
        df = read_csv_data(file_path)
        validate_columns(df)

        # 增量模式：仅保留 since_date 之后的数据
        if since_date:
            df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
            before = len(df)
            df = df[df["InvoiceDate"] >= since_date]
            logger.info(
                "增量过滤: %d → %d 行 (since %s)", before, len(df), since_date,
            )
            if df.empty:
                return {
                    "task": "extract_orders",
                    "status": "skipped",
                    "reason": f"无 {since_date} 之后的新增数据",
                    "duration": (datetime.now() - start).total_seconds(),
                }

        row_count = load_to_ods(df, incremental=incremental)
        return {
            "task": "extract_orders",
            "status": "success",
            "rows": row_count,
            "duration": (datetime.now() - start).total_seconds(),
        }
    except Exception as e:
        logger.error("extract_orders 失败: %s", e)
        return {
            "task": "extract_orders",
            "status": "failed",
            "error": str(e),
            "duration": (datetime.now() - start).total_seconds(),
        }


def auto_extract(file_path: str, schema_yaml: str = None) -> dict:
    """
    Step 1 自动模式入口：任意 CSV → ODS（零配置）

    流程:
      1. sanitizer 修复文件结构 → DataFrame
      2. profiler 推断列角色 → RuntimeSchema
      3. (可选) YAML schema 覆盖推断结果
      4. 日期列统一转换
      5. 动态建表 + 写入 ODS

    Args:
        file_path: 任意 CSV 文件路径
        schema_yaml: 可选的 YAML schema 覆盖文件路径

    Returns:
        执行结果摘要 (兼容 run_extract 返回格式)
    """
    start = datetime.now()
    try:
        # --- S 层: 文件修复 + 读取 ---
        yaml_raw = load_yaml_schema(schema_yaml)
        yaml_schema = yaml_to_runtime_schema(yaml_raw) if yaml_raw else RuntimeSchema()

        dtype_overrides = yaml_schema.csv_dtype_overrides or None
        df = read_csv_safe(file_path, dtype_overrides=dtype_overrides)
        logger.info("S层读取: %d 行 × %d 列", len(df), len(df.columns))

        # --- P 层: 列推断 ---
        inferred = profile_dataframe(df)
        schema = merge_schemas(yaml_schema, inferred)

        # --- 日期列统一转换 ---
        df = infer_and_clean_dates(df, schema.date_columns)

        # --- 加载到 ODS ---
        row_count = dynamic_load("ods_orders", df)

        # 缓存推断结果
        save_cache(schema)

        return {
            "task": "auto_extract",
            "status": "success",
            "rows": row_count,
            "columns": len(df.columns),
            "schema": {
                "date_columns": schema.date_columns,
                "numeric_columns": schema.numeric_columns,
                "categorical_columns": schema.categorical_columns,
                "id_columns": schema.id_columns,
                "bool_columns": schema.bool_columns,
            },
            "duration": (datetime.now() - start).total_seconds(),
        }
    except Exception as e:
        logger.error("auto_extract 失败: %s", e, exc_info=True)
        return {
            "task": "auto_extract",
            "status": "failed",
            "error": str(e),
            "duration": (datetime.now() - start).total_seconds(),
        }



if __name__ == "__main__":
    import sys

    data_path = sys.argv[1] if len(sys.argv) > 1 else "data/online_retail.csv"
    # 优先尝试自动模式
    result = auto_extract(data_path)
    print(result)
