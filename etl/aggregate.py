"""
数据聚合模块 (Aggregate)
========================
Step 4: DWD → DWS，按日汇总销售主题指标。

核心指标：
- daily_order_count      日订单数
- daily_customer_count   日客户数
- daily_sales_amount     日销售额
- daily_avg_order_amount 日均客单价
"""

import logging
from datetime import datetime
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from etl.db import get_engine

logger = logging.getLogger(__name__)

# 模块级表定义 — 避免每次调用 autoload 查询 information_schema
_dws_table = sa.Table(
    "dws_sales_daily",
    sa.MetaData(),
    sa.Column("stat_date", sa.Date, primary_key=True),
    sa.Column("daily_order_count", sa.Integer, nullable=False),
    sa.Column("daily_customer_count", sa.Integer, nullable=False),
    sa.Column("daily_sales_amount", sa.Numeric(14, 4), nullable=False),
    sa.Column("daily_avg_order_amount", sa.Numeric(12, 4), nullable=False),
    sa.Column("etl_time", sa.DateTime, nullable=False),
)


def load_dwd_data(engine) -> pd.DataFrame:
    """从 DWD 层读取订单明细"""
    query = """
        SELECT
            DATE(invoice_date)   AS stat_date,
            invoice_no,
            customer_id,
            order_amount
        FROM dwd_orders
    """
    df = pd.read_sql(query, engine)
    logger.info("从 DWD 读取: %d 行", len(df))
    return df


def aggregate_daily(df: pd.DataFrame) -> pd.DataFrame:
    """
    按日聚合销售指标

    指标计算逻辑:
    - daily_order_count:      去重订单数
    - daily_customer_count:   去重客户数
    - daily_sales_amount:     订单金额求和
    - daily_avg_order_amount: 销售额 / 订单数
    """
    daily = df.groupby("stat_date").agg(
        daily_order_count=("invoice_no", "nunique"),
        daily_customer_count=("customer_id", "nunique"),
        daily_sales_amount=("order_amount", "sum"),
    ).reset_index()

    # 客单价 = 销售额 / 订单数
    daily["daily_avg_order_amount"] = (
        daily["daily_sales_amount"] / daily["daily_order_count"]
    )

    # 四舍五入保留 4 位小数
    daily["daily_sales_amount"] = daily["daily_sales_amount"].round(4)
    daily["daily_avg_order_amount"] = daily["daily_avg_order_amount"].round(4)

    logger.info("聚合完成: %d 个日期", len(daily))
    return daily


def load_to_dws(df: pd.DataFrame) -> int:
    """
    将汇总数据写入 DWS 层表 dws_sales_daily

    使用 SQLAlchemy 批量 UPSERT (INSERT ON CONFLICT DO UPDATE)，
    单条 SQL 完成所有行的写入，避免 N+1 查询。
    """
    engine = get_engine()

    # 使用模块级缓存的表定义，避免每次 autoload 查询 information_schema
    table = _dws_table

    records = df.to_dict("records")
    if not records:
        return 0

    stmt = pg_insert(table).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=["stat_date"],
        set_={
            "daily_order_count": stmt.excluded.daily_order_count,
            "daily_customer_count": stmt.excluded.daily_customer_count,
            "daily_sales_amount": stmt.excluded.daily_sales_amount,
            "daily_avg_order_amount": stmt.excluded.daily_avg_order_amount,
            "etl_time": sa.text("NOW()"),
        },
    )

    with engine.begin() as conn:
        conn.execute(stmt)

    logger.info("DWS 批量写入完成: %d 行", len(records))
    return len(records)


def run_aggregate() -> dict:
    """
    Step 4 入口：DWD → DWS 聚合

    Returns:
        执行结果摘要
    """
    start = datetime.now()
    try:
        engine = get_engine()
        df_dwd = load_dwd_data(engine)

        if df_dwd.empty:
            return {
                "task": "build_dws",
                "status": "skipped",
                "reason": "DWD 表为空，请先执行 transform 步骤",
                "rows": 0,
                "duration": (datetime.now() - start).total_seconds(),
            }

        df_agg = aggregate_daily(df_dwd)
        row_count = load_to_dws(df_agg)
        return {
            "task": "build_dws",
            "status": "success",
            "rows": row_count,
            "duration": (datetime.now() - start).total_seconds(),
        }
    except Exception as e:
        return {
            "task": "build_dws",
            "status": "failed",
            "error": str(e),
            "duration": (datetime.now() - start).total_seconds(),
        }


if __name__ == "__main__":
    result = run_aggregate()
    print(result)
