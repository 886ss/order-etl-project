"""
数据转换清洗模块 (Transform)
============================
Step 3: ODS → DWD，完成数据清洗与明细加工。

双模式:
  - run_transform():   YAML schema 驱动（UCI 兼容）
  - auto_transform():  运行时 schema 驱动（任意数据集，零配置）

处理内容:
  1. 取消/退货检测（按 schema.cancel_rules 配置）
  2. 必填列空值过滤
  3. 值域校验
  4. 计算列（如 order_amount = quantity * unit_price）
  5. 日期统一
  6. 脏数据归档
"""

from datetime import datetime
import logging
import pandas as pd
from etl.db import (
    get_engine, text, DWD_DTYPE, REJECTED_DTYPE,
    truncate_and_load, append_to_table, upsert_incremental, dynamic_load,
)
from etl.config import RuntimeSchema, load_yaml_schema, yaml_to_runtime_schema, merge_schemas
from etl.profiler import profile_dataframe, infer_and_clean_dates

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


# ============================================================
# 自动模式: 通用清洗（运行时 schema 驱动）
# ============================================================

def clean_data_auto(df: pd.DataFrame, schema: RuntimeSchema) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    通用清洗引擎：根据 RuntimeSchema 的清洗规则处理任意数据集。

    与 clean_data() 不同：
      - 取消检测规则从 schema.cancel_rules 读取，不硬编码
      - 所有 .str 操作前先 astype(str)，防止 int 列崩溃 (BUG-002)
      - 必填列/值域校验/计算列全部由 schema 驱动
      - 保留 source_mapping 中未参与清洗的列（不丢弃）

    Returns:
        (cleaned_df, rejected_df)
    """
    original_count = len(df)
    combined_mask = pd.Series(False, index=df.index)
    reasons: list[tuple[pd.Series, str]] = []  # (mask, reason)

    _OPS = {
        "gt": lambda s, v: s <= v, "gte": lambda s, v: s < v,
        "lt": lambda s, v: s >= v, "lte": lambda s, v: s > v,
        "eq": lambda s, v: s != v, "neq": lambda s, v: s == v,
    }

    # 1. 取消/退货规则 (BUG-002: lazy astype(str) only when .str is needed)
    for rule in schema.cancel_rules:
        col = rule["column"]
        if col not in df.columns:
            continue
        method = rule.get("method", "")
        reason = rule.get("reason", f"取消: {method}")

        mask = pd.Series(False, index=df.index)
        if method == "string_startswith":
            s = df[col] if df[col].dtype == "object" else df[col].astype(str)
            mask = s.str.startswith(str(rule.get("value", "")), na=False)
        elif method == "string_equals":
            s = df[col] if df[col].dtype == "object" else df[col].astype(str)
            mask = s.str.lower() == str(rule.get("value", "")).lower()
        elif method == "column_equals":
            mask = df[col] == rule.get("value")

        if mask.any():
            combined_mask |= mask
            reasons.append((mask, reason))
            logger.info("规则 [%s]: 标记 %d 行", reason, mask.sum())

    # 2. 必填列
    for req in schema.required_columns:
        col = req["column"]
        if col not in df.columns:
            continue
        null_mask = df[col].isna()
        if null_mask.any():
            combined_mask |= null_mask
            reason = req.get("reason", f"{col} 为空")
            reasons.append((null_mask, reason))
            logger.info("必填 [%s]: 标记 %d 行", col, null_mask.sum())

    # 3. 值域校验
    for val in schema.validations:
        col = val["column"]
        if col not in df.columns:
            continue
        op_func = _OPS.get(val.get("operator", "gt"))
        if op_func is None:
            continue
        threshold = val.get("value", 0)
        bad_mask = op_func(df[col], threshold)
        if bad_mask.any():
            combined_mask |= bad_mask
            reason = val.get("reason", f"{col} {val.get('operator')} {threshold}")
            reasons.append((bad_mask, reason))
            logger.info("校验 [%s]: 标记 %d 行", reason, bad_mask.sum())

    # 一次性拆分（避免多次 df=df[~mask] 产生新的分配）
    if combined_mask.any():
        rejected_rows = [df[mask].assign(rejected_reason=reason) for mask, reason in reasons]
        rejected_parts = rejected_rows
        df = df[~combined_mask]
    else:
        rejected_parts = []

    # 4. 计算列
    for comp in schema.computed_columns:
        name = comp["name"]
        expr = comp["expression"]
        try:
            # 安全 eval：只允许基本算术
            df[name] = df.eval(expr, engine="python")
            logger.info("计算列 [%s] = %s", name, expr)
        except Exception as e:
            logger.warning("计算列 [%s] 执行失败: %s", name, e)

    # 5. ETL 时间戳
    df["etl_time"] = datetime.now()

    # 汇总
    cleaned_count = len(df)
    removed = original_count - cleaned_count
    logger.info(
        "清洗完成: %d → %d 行 (剔除 %d 行, %.1f%%)",
        original_count, cleaned_count, removed,
        removed / original_count * 100 if original_count else 0,
    )

    rejected_df = pd.concat(rejected_parts, ignore_index=True) if rejected_parts else pd.DataFrame()
    return df, rejected_df


def auto_transform(schema_yaml: str = None) -> dict:
    """
    Step 3 自动模式入口：ODS → DWD（零配置）

    从 ODS 表读取数据，运行通用清洗引擎，
    将干净数据写入 DWD，脏数据归档。

    Args:
        schema_yaml: 可选的 YAML schema 路径

    Returns:
        执行结果摘要 (兼容 run_transform 返回格式)
    """
    start = datetime.now()
    try:
        engine = get_engine()
        yaml_raw = load_yaml_schema(schema_yaml)
        yaml_schema = yaml_to_runtime_schema(yaml_raw) if yaml_raw else RuntimeSchema()

        # 读 ODS
        df_ods = pd.read_sql("SELECT * FROM ods_orders", engine)
        if df_ods.empty:
            return {
                "task": "auto_transform",
                "status": "skipped",
                "reason": "ODS 表为空",
                "duration": (datetime.now() - start).total_seconds(),
            }

        # 推断 + 合并 schema
        inferred = profile_dataframe(df_ods)
        schema = merge_schemas(yaml_schema, inferred) if yaml_raw else inferred

        # 日期列统一
        df_ods = infer_and_clean_dates(df_ods, schema.date_columns)

        # 清洗
        df_dwd, rejected_df = clean_data_auto(df_ods, schema)
        if not rejected_df.empty:
            archive_rejected(rejected_df)

        # 写入 DWD
        row_count = dynamic_load("dwd_orders", df_dwd)

        return {
            "task": "auto_transform",
            "status": "success",
            "rows": row_count,
            "rejected": len(rejected_df),
            "duration": (datetime.now() - start).total_seconds(),
        }
    except Exception as e:
        logger.error("auto_transform 失败: %s", e, exc_info=True)
        return {
            "task": "auto_transform",
            "status": "failed",
            "error": str(e),
            "duration": (datetime.now() - start).total_seconds(),
        }


