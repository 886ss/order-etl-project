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
from etl.db import get_engine, text, DWD_DTYPE, truncate_and_load, append_to_table

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
    """
    original_count = len(df)

    # 1. 过滤取消订单（InvoiceNo 以 'C' 开头的是取消/退货）
    cancel_mask = df["invoice_no"].str.startswith("C", na=False)
    cancel_count = cancel_mask.sum()
    df = df[~cancel_mask]
    logger.info("过滤取消订单: %d 行", cancel_count)

    # 2. 去除 CustomerID 为空
    null_cust = df["customer_id"].isna().sum()
    df = df.dropna(subset=["customer_id"])
    logger.info("去除空客户ID: %d 行", null_cust)

    # 3. 去除 Quantity ≤ 0
    neg_qty = (df["quantity"] <= 0).sum()
    df = df[df["quantity"] > 0]
    logger.info("去除非正数量: %d 行", neg_qty)

    # 4. 去除 UnitPrice ≤ 0
    neg_price = (df["unit_price"] <= 0).sum()
    df = df[df["unit_price"] > 0]
    logger.info("去除非正单价: %d 行", neg_price)

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

    return df


def load_to_dwd(df: pd.DataFrame, incremental: bool = False) -> int:
    """
    将清洗后数据写入 DWD 层表 dwd_orders

    全量模式（默认）：TRUNCATE + INSERT 同事务。
    增量模式：追加写入，不截断已有数据。

    Args:
        df: 清洗后的 DataFrame
        incremental: True 时使用追加模式

    Returns:
        写入行数
    """
    dwd_columns = list(DWD_DTYPE.keys())
    df_dwd = df[dwd_columns]

    if incremental:
        count = append_to_table("dwd_orders", df_dwd, DWD_DTYPE)
        logger.info("DWD 增量追加: %d 行", count)
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

        df_dwd = clean_data(df_ods)
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
