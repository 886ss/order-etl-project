"""
数据质量检查模块 (Quality Check)
================================
Step 2: 对 ODS 层数据进行质量检查。
检查项：空值、重复值、异常金额、取消订单标记。
"""

from datetime import datetime
from sqlalchemy import text
from etl.db import get_engine


def check_null_values(engine) -> dict:
    """
    检查关键字段空值情况

    Returns:
        各字段空值计数
    """
    query = """
        SELECT
            COUNT(*)                                              AS total_rows,
            COUNT(*) FILTER (WHERE invoice_no IS NULL)            AS null_invoice_no,
            COUNT(*) FILTER (WHERE stock_code IS NULL)            AS null_stock_code,
            COUNT(*) FILTER (WHERE quantity IS NULL)              AS null_quantity,
            COUNT(*) FILTER (WHERE invoice_date IS NULL)          AS null_invoice_date,
            COUNT(*) FILTER (WHERE unit_price IS NULL)            AS null_unit_price,
            COUNT(*) FILTER (WHERE customer_id IS NULL)           AS null_customer_id,
            COUNT(*) FILTER (WHERE country IS NULL)               AS null_country
        FROM ods_orders
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
    return dict(result._mapping)


def check_duplicates(engine) -> dict:
    """
    检查重复订单

    Returns:
        重复统计信息
    """
    query = """
        SELECT
            COUNT(*)                                   AS total_rows,
            COUNT(DISTINCT invoice_no)                 AS unique_invoice_no,
            COUNT(*) - COUNT(DISTINCT invoice_no)      AS duplicate_invoice_count
        FROM ods_orders
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
    return dict(result._mapping)


def check_abnormal_amounts(engine) -> dict:
    """
    检查异常金额数据：负单价、负数量、零单价

    Returns:
        异常值统计
    """
    query = """
        SELECT
            COUNT(*) FILTER (WHERE unit_price < 0)      AS negative_price_count,
            COUNT(*) FILTER (WHERE quantity < 0)        AS negative_quantity_count,
            COUNT(*) FILTER (WHERE unit_price = 0)      AS zero_price_count,
            COUNT(*) FILTER (WHERE quantity = 0)        AS zero_quantity_count
        FROM ods_orders
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
    return dict(result._mapping)


def run_quality_check() -> dict:
    """
    Step 2 入口：执行完整数据质量检查

    Returns:
        质量检查报告
    """
    start = datetime.now()
    engine = get_engine()

    try:
        null_info = check_null_values(engine)
        dup_info = check_duplicates(engine)
        abnormal_info = check_abnormal_amounts(engine)

        # 判定是否通过
        has_critical = (
            null_info["null_invoice_no"] > 0
            or null_info["null_quantity"] > 0
            or null_info["null_invoice_date"] > 0
            or null_info["null_unit_price"] > 0
        )
        status = "warning" if has_critical else "passed"

        report = {
            "task": "check_quality",
            "status": status,
            "null_check": null_info,
            "duplicate_check": dup_info,
            "abnormal_check": abnormal_info,
            "duration": (datetime.now() - start).total_seconds(),
        }

        print(f"[quality] 空值检查: {null_info}")
        print(f"[quality] 重复检查: {dup_info}")
        print(f"[quality] 异常值检查: {abnormal_info}")
        print(f"[quality] 质量检查结果: {status}")

        return report

    except Exception as e:
        return {
            "task": "check_quality",
            "status": "failed",
            "error": str(e),
            "duration": (datetime.now() - start).total_seconds(),
        }


if __name__ == "__main__":
    result = run_quality_check()
    print(result)
