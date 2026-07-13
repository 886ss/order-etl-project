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
from etl.db import get_engine, text, DWD_DTYPE, truncate_and_load

logger = logging.getLogger(__name__)


def load_ods_data(engine) -> pd.DataFrame:
    """从 ODS 层读取全量数据"""
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


def load_to_dwd(df: pd.DataFrame) -> int:
    """
    将清洗后数据写入 DWD 层表 dwd_orders

    TRUNCATE 和 INSERT 在同一事务中执行，保证原子性。

    Args:
        df: 清洗后的 DataFrame

    Returns:
        写入行数
    """
    dwd_columns = list(DWD_DTYPE.keys())
    df_dwd = df[dwd_columns]

    count = truncate_and_load("dwd_orders", df_dwd, DWD_DTYPE)
    logger.info("DWD 写入完成: %d 行", count)
    return count


def run_transform() -> dict:
    """
    Step 3 入口：ODS → DWD 转换

    Returns:
        执行结果摘要
    """
    start = datetime.now()
    try:
        engine = get_engine()
        df_ods = load_ods_data(engine)

        if df_ods.empty:
            return {
                "task": "build_dwd",
                "status": "skipped",
                "reason": "ODS 表为空，请先执行 extract 步骤",
                "rows": 0,
                "duration": (datetime.now() - start).total_seconds(),
            }

        df_dwd = clean_data(df_ods)
        row_count = load_to_dwd(df_dwd)
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
