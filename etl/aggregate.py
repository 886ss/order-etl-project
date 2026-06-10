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

from datetime import datetime
import pandas as pd
from sqlalchemy import text
from etl.db import get_engine


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
    print(f"[aggregate] 从 DWD 读取: {len(df)} 行")
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

    print(f"[aggregate] 聚合完成: {len(daily)} 个日期")
    return daily


def load_to_dws(df: pd.DataFrame) -> int:
    """
    将汇总数据写入 DWS 层表 dws_sales_daily

    使用 UPSERT 策略：新日期插入，已存在则更新。
    """
    engine = get_engine()
    rows_upserted = 0

    with engine.begin() as conn:
        for _, row in df.iterrows():
            upsert_sql = """
                INSERT INTO dws_sales_daily
                    (stat_date, daily_order_count, daily_customer_count,
                     daily_sales_amount, daily_avg_order_amount, etl_time)
                VALUES (:sd, :doc, :dcc, :dsa, :daoa, NOW())
                ON CONFLICT (stat_date)
                DO UPDATE SET
                    daily_order_count      = EXCLUDED.daily_order_count,
                    daily_customer_count   = EXCLUDED.daily_customer_count,
                    daily_sales_amount     = EXCLUDED.daily_sales_amount,
                    daily_avg_order_amount = EXCLUDED.daily_avg_order_amount,
                    etl_time               = NOW()
            """
            conn.execute(text(upsert_sql), {
                "sd": row["stat_date"],
                "doc": int(row["daily_order_count"]),
                "dcc": int(row["daily_customer_count"]),
                "dsa": float(row["daily_sales_amount"]),
                "daoa": float(row["daily_avg_order_amount"]),
            })
            rows_upserted += 1

    print(f"[aggregate] DWS 写入完成: {rows_upserted} 行")
    return rows_upserted


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
