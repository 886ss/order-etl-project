"""
数据质量检查模块 (Quality Check)
================================
Step 2: 对 ODS 层数据进行质量检查。

检查项：
1. 空值检查  — 关键字段（invoice_no/quantity/invoice_date/unit_price）任一为空 → critical
2. 空值率预警 — 非关键字段空值率超过阈值时写 warning 日志，不中断流程
3. 重复检查  — invoice_no 重复统计
4. 异常金额  — 负单价/负数量/零单价/零数量

预警不中断机制：非关键字段空值率超过阈值时，
仅记录 warning 日志，管线继续执行，兼顾日报产出与数据质量追溯。
"""

import logging
import os
from datetime import datetime

from dotenv import load_dotenv

from etl.db import get_engine, text

load_dotenv()
logger = logging.getLogger(__name__)

# 非关键字段空值率阈值（超过即预警，不中断）
_NULL_RATE_THRESHOLD = float(os.getenv("QUALITY_NULL_THRESHOLD", "0.30"))


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


def check_null_rate_warnings(engine, threshold: float = None) -> dict:
    """
    非关键字段空值率预警（不中断流程）

    对 description、stock_code、country 等非关键字段计算空值率，
    超过阈值的字段写入 warning 日志但不阻断管线。

    Args:
        engine: SQLAlchemy 引擎
        threshold: 空值率阈值，默认使用全局配置 _NULL_RATE_THRESHOLD

    Returns:
        {"threshold": float, "warnings": [{"field": str, "null_rate": float}, ...]}
    """
    if threshold is None:
        threshold = _NULL_RATE_THRESHOLD

    query = """
        SELECT
            COUNT(*)                                                         AS total_rows,
            CAST(SUM(CASE WHEN description IS NULL THEN 1 ELSE 0 END) AS REAL)
                / NULLIF(COUNT(*), 0)                                        AS null_rate_description,
            CAST(SUM(CASE WHEN stock_code IS NULL THEN 1 ELSE 0 END) AS REAL)
                / NULLIF(COUNT(*), 0)                                        AS null_rate_stock_code,
            CAST(SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS REAL)
                / NULLIF(COUNT(*), 0)                                        AS null_rate_country
        FROM ods_orders
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
        row = dict(result._mapping)

    total = row.pop("total_rows", 0)
    warnings = []
    for field, rate in row.items():
        # field name format: null_rate_{actual_field_name}
        field_name = field.replace("null_rate_", "")
        if rate is not None and rate > threshold:
            warnings.append({"field": field_name, "null_rate": round(rate, 4)})
            logger.warning(
                "⚠ 空值率预警: %s = %.2f%% (阈值 %.0f%%) — 不中断流程",
                field_name, rate * 100, threshold * 100,
            )

    if not warnings:
        logger.info("空值率检查通过: 所有非关键字段空值率 ≤ %.0f%%", threshold * 100)

    return {"threshold": threshold, "warnings": warnings, "total_rows": total}


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

        # 非关键字段空值率预警（不中断流程）
        null_rate_info = check_null_rate_warnings(engine)

        # 判定是否通过：仅关键字段空值触发 critical
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
            "null_rate_check": null_rate_info,
            "duration": (datetime.now() - start).total_seconds(),
        }

        logger.info("空值检查: %s", null_info)
        logger.info("重复检查: %s", dup_info)
        logger.info("异常值检查: %s", abnormal_info)
        logger.info("空值率预警: 阈值=%.0f%%, 超限字段=%d",
                     null_rate_info["threshold"] * 100,
                     len(null_rate_info["warnings"]))
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
