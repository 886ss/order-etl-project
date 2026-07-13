"""
数据质量检查模块 (Quality Check)
================================
Step 2: 对 ODS 层数据进行质量检查。
检查项：空值、重复值、异常金额、取消订单标记。
"""

import logging
from datetime import datetime
from etl.db import get_engine, text

logger = logging.getLogger(__name__)


def check_null_values(engine) -> dict:
    """
    检查关键字段空值情况

    使用 CASE WHEN 而非 FILTER 子句，兼容 PostgreSQL 和 SQLite。

    Returns:
        各字段空值计数
    """
    query = """
        SELECT
            COUNT(*)                                                       AS total_rows,
            SUM(CASE WHEN invoice_no IS NULL THEN 1 ELSE 0 END)            AS null_invoice_no,
            SUM(CASE WHEN stock_code IS NULL THEN 1 ELSE 0 END)            AS null_stock_code,
            SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END)              AS null_quantity,
            SUM(CASE WHEN invoice_date IS NULL THEN 1 ELSE 0 END)          AS null_invoice_date,
            SUM(CASE WHEN unit_price IS NULL THEN 1 ELSE 0 END)            AS null_unit_price,
            SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)           AS null_customer_id,
            SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END)               AS null_country
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
            SUM(CASE WHEN unit_price < 0 THEN 1 ELSE 0 END)      AS negative_price_count,
            SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END)        AS negative_quantity_count,
            SUM(CASE WHEN unit_price = 0 THEN 1 ELSE 0 END)      AS zero_price_count,
            SUM(CASE WHEN quantity = 0 THEN 1 ELSE 0 END)        AS zero_quantity_count
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

        # 空表视为检查失败，而非"通过"
        if null_info.get("total_rows", 0) == 0:
            return {
                "task": "check_quality",
                "status": "failed",
                "error": "ODS 表为空，请先执行 extract 步骤",
                "duration": (datetime.now() - start).total_seconds(),
            }

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

        logger.info("空值检查: %s", null_info)
        logger.info("重复检查: %s", dup_info)
        logger.info("异常值检查: %s", abnormal_info)
        logger.info("质量检查结果: %s", status)

        return report

    except Exception as e:
        logger.error("check_quality 失败: %s", e)
        return {
            "task": "check_quality",
            "status": "failed",
            "error": str(e),
            "duration": (datetime.now() - start).total_seconds(),
        }


if __name__ == "__main__":
    result = run_quality_check()
    print(result)
