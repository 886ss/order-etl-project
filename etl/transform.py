"""
数据转换清洗模块 (Transform)
============================
Step 3: ODS → DWD，完成数据清洗与明细加工。

处理内容：
1. 过滤取消订单（InvoiceNo 以 'C' 开头）
2. 剔除 CustomerID 为空的记录
3. 剔除异常金额（Quantity ≤ 0 或 UnitPrice ≤ 0）
4. 计算 order_amount = Quantity × UnitPrice
5. 统一时间格式
6. 记录 etl_time
"""

from datetime import datetime
import logging
import pandas as pd
from etl.db import (
    get_engine, text, DWD_DTYPE, REJECTED_DTYPE,
    truncate_and_load, append_to_table, upsert_incremental,
)

logger = logging.getLogger(__name__)


def load_ods_data(engine, since_date: str = None) -> pd.DataFrame:
    """
    从 ODS 层读取数据

    增量模式下仅读取 since_date 之后的记录，
    避免对全量 ODS 重复清洗。
    """
    if since_date:
        query = "SELECT * FROM ods_orders WHERE invoice_date >= :since"
        df = pd.read_sql(query, engine, params={"since": since_date})
    else:
        query = "SELECT * FROM ods_orders"
        df = pd.read_sql(query, engine)
    logger.info("从 ODS 读取: %d 行", len(df))
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    数据清洗流水线

    步骤:
    1. 过滤取消订单 (InvoiceNo 以 'C' 开头)
    2. 去除 CustomerID 为空的记录
    3. 去除 Quantity ≤ 0 的记录
    4. 去除 UnitPrice ≤ 0 的记录
    5. 计算订单金额
    6. 添加 etl_time

    被剔除的行返回给调用方，由调用方负责归档到 rejected 表。
    """
    original_count = len(df)
    rejected_parts = []

    # 1. 过滤取消订单
    cancel_mask = df["invoice_no"].str.startswith("C", na=False)
    rejected_parts.append(df[cancel_mask].assign(rejected_reason="取消订单 (InvoiceNo以C开头)"))
    df = df[~cancel_mask]
    logger.info("过滤取消订单: %d 行", cancel_mask.sum())

    # 2. 去除 CustomerID 为空
    null_cust_mask = df["customer_id"].isna()
    rejected_parts.append(df[null_cust_mask].assign(rejected_reason="CustomerID 为空"))
    df = df[~null_cust_mask]
    logger.info("去除空客户ID: %d 行", null_cust_mask.sum())

    # 3. 去除 Quantity ≤ 0
    neg_qty_mask = df["quantity"] <= 0
    rejected_parts.append(df[neg_qty_mask].assign(rejected_reason="Quantity ≤ 0"))
    df = df[df["quantity"] > 0]
    logger.info("去除非正数量: %d 行", neg_qty_mask.sum())

    # 4. 去除 UnitPrice ≤ 0
    neg_price_mask = df["unit_price"] <= 0
    rejected_parts.append(df[neg_price_mask].assign(rejected_reason="UnitPrice ≤ 0"))
    df = df[df["unit_price"] > 0]
    logger.info("去除非正单价: %d 行", neg_price_mask.sum())

    # 5. 计算订单金额
    df["order_amount"] = df["quantity"] * df["unit_price"]

    # 6. 添加 ETL 时间戳
    df["etl_time"] = datetime.now()

    cleaned_count = len(df)
    removed = original_count - cleaned_count
    logger.info(
        "清洗完成: %d → %d 行 (剔除 %d 行, %.1f%%)",
        original_count, cleaned_count, removed,
        removed / original_count * 100 if original_count else 0,
    )

    # 拼接被剔除行
    rejected_df = pd.concat(rejected_parts, ignore_index=True) if rejected_parts else pd.DataFrame()
    return df, rejected_df


def archive_rejected(rejected_df: pd.DataFrame) -> int:
    """
    将清洗过程中被剔除的数据归档到 ods_orders_rejected 表。

    每一行标记拒绝原因（rejected_reason）和归档时间（rejected_at），
    方便后续人工核查和数据治理审计。
    """
    if rejected_df.empty:
        return 0

    rejected_df = rejected_df.copy()
    rejected_df["rejected_at"] = datetime.now()
    rejected_cols = [c for c in REJECTED_DTYPE.keys() if c in rejected_df.columns]
    df_rej = rejected_df[rejected_cols]

    count = append_to_table("ods_orders_rejected", df_rej, REJECTED_DTYPE)
    logger.info("脏数据归档: %d 行 → ods_orders_rejected", count)
    return count


def load_to_dwd(df: pd.DataFrame, incremental: bool = False) -> int:
    """
    将清洗后数据写入 DWD 层表 dwd_orders

    全量模式（默认）：TRUNCATE + INSERT 同事务。
    增量模式：幂等追加（DELETE 日期批次 + INSERT），Airflow 重跑不重复。

    Args:
        df: 清洗后的 DataFrame
        incremental: True 时使用幂等增量模式

    Returns:
        写入行数
    """
    dwd_columns = list(DWD_DTYPE.keys())
    df_dwd = df[dwd_columns]

    if incremental:
        count = upsert_incremental("dwd_orders", df_dwd, DWD_DTYPE, "invoice_date")
        logger.info("DWD 增量写入（幂等）: %d 行", count)
    else:
        count = truncate_and_load("dwd_orders", df_dwd, DWD_DTYPE)
        logger.info("DWD 全量写入: %d 行", count)
    return count


def run_transform(incremental: bool = False, since_date: str = None) -> dict:
    """
    Step 3 入口：ODS → DWD 转换

    Args:
        incremental: True 时仅处理 since_date 之后的数据，追加到 DWD
        since_date: 增量处理的起始日期（YYYY-MM-DD）

    Returns:
        执行结果摘要
    """
    start = datetime.now()
    try:
        engine = get_engine()
        df_ods = load_ods_data(engine, since_date=since_date if incremental else None)

        if df_ods.empty:
            reason = (
                f"无 {since_date} 之后的新增数据"
                if incremental
                else "ODS 表为空，请先执行 extract 步骤"
            )
            return {
                "task": "build_dwd",
                "status": "skipped",
                "reason": reason,
                "rows": 0,
                "duration": (datetime.now() - start).total_seconds(),
            }

        df_dwd, rejected_df = clean_data(df_ods)
        archive_rejected(rejected_df)
        row_count = load_to_dwd(df_dwd, incremental=incremental)
        return {
            "task": "build_dwd",
            "status": "success",
            "rows": row_count,
            "duration": (datetime.now() - start).total_seconds(),
        }
    except Exception as e:
        logger.error("build_dwd 失败: %s", e)
        return {
            "task": "build_dwd",
            "status": "failed",
            "error": str(e),
            "duration": (datetime.now() - start).total_seconds(),
        }


if __name__ == "__main__":
    result = run_transform()
    print(result)
