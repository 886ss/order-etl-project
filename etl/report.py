"""
日报生成模块 (Report)
====================
Step 5: 从 DWS 层读取汇总数据，生成每日运营日报。

输出内容：
- 销售额、订单数、客户数、客单价
- 日报 CSV 文件
"""

import os
import logging
from datetime import datetime
import pandas as pd
from etl.db import get_engine, text

logger = logging.getLogger(__name__)


def load_dws_data(engine) -> pd.DataFrame:
    """从 DWS 层读取日度汇总数据，按日期排序"""
    query = """
        SELECT
            stat_date,
            daily_order_count,
            daily_customer_count,
            daily_sales_amount,
            daily_avg_order_amount
        FROM dws_sales_daily
        ORDER BY stat_date DESC
    """
    df = pd.read_sql(query, engine)
    logger.info("从 DWS 读取: %d 行", len(df))
    return df


def generate_summary(df: pd.DataFrame) -> dict:
    """
    生成汇总统计

    Returns:
        关键指标汇总字典
    """
    if df.empty:
        return {"message": "无数据"}

    total_sales = df["daily_sales_amount"].sum()
    total_orders = df["daily_order_count"].sum()
    total_customers = df["daily_customer_count"].sum()
    avg_daily_sales = df["daily_sales_amount"].mean()
    avg_order_amount = total_sales / total_orders if total_orders else 0

    summary = {
        "统计天数": len(df),
        "累计销售额": round(total_sales, 2),
        "累计订单数": int(total_orders),
        "累计客户数": int(total_customers),
        "日均销售额": round(avg_daily_sales, 2),
        "平均客单价": round(avg_order_amount, 2),
        "最高单日销售额": round(df["daily_sales_amount"].max(), 2),
        "最高单日订单数": int(df["daily_order_count"].max()),
    }
    return summary


def save_report(df: pd.DataFrame, summary: dict, output_dir: str = "reports") -> str:
    """
    保存日报 CSV 文件

    Args:
        df: DWS 汇总数据
        summary: 汇总指标
        output_dir: 输出目录

    Returns:
        输出文件路径
    """
    os.makedirs(output_dir, exist_ok=True)

    # 日报文件名带日期
    today = datetime.now().strftime("%Y%m%d")
    report_path = os.path.join(output_dir, f"daily_report_{today}.csv")

    # 注释头 + CSV 数据，单次文件打开避免重复 I/O
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"# 订单数据日报 — 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        for key, val in summary.items():
            f.write(f"# {key}: {val}\n")
        f.write("#\n")
        df.to_csv(f, index=False)

    logger.info("日报已保存: %s", report_path)
    return report_path


def print_report(summary: dict, top_n: int = 10):
    """在控制台打印日报摘要"""
    print("\n" + "=" * 60)
    print("  📊 订单数据日报")
    print("=" * 60)
    for key, val in summary.items():
        print(f"  {key}: {val}")
    print("=" * 60 + "\n")


def run_report(output_dir: str = "reports") -> dict:
    """
    Step 5 入口：生成日报

    Returns:
        执行结果摘要
    """
    start = datetime.now()
    try:
        engine = get_engine()
        df_dws = load_dws_data(engine)

        if df_dws.empty:
            return {
                "task": "generate_report",
                "status": "warning",
                "message": "DWS 层无数据，跳过日报生成",
                "duration": (datetime.now() - start).total_seconds(),
            }

        summary = generate_summary(df_dws)
        report_path = save_report(df_dws, summary, output_dir)
        print_report(summary)

        return {
            "task": "generate_report",
            "status": "success",
            "report_path": report_path,
            "summary": summary,
            "duration": (datetime.now() - start).total_seconds(),
        }
    except Exception as e:
        logger.error("generate_report 失败: %s", e)
        return {
            "task": "generate_report",
            "status": "failed",
            "error": str(e),
            "duration": (datetime.now() - start).total_seconds(),
        }


if __name__ == "__main__":
    result = run_report()
    print(result)
