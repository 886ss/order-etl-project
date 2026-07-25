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
from etl.db import get_engine, dynamic_load
from etl.config import RuntimeSchema, load_yaml_schema, yaml_to_runtime_schema, merge_schemas
from etl.profiler import profile_dataframe

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
        logger.error("build_dws 失败: %s", e)
        return {
            "task": "build_dws",
            "status": "failed",
            "error": str(e),
            "duration": (datetime.now() - start).total_seconds(),
        }


if __name__ == "__main__":
    result = run_aggregate()
    print(result)


# ============================================================
# 自动模式: 自适应聚合（日期 × 数值 × 分类 三维驱动）
# ============================================================

def auto_aggregate(df: pd.DataFrame, schema: RuntimeSchema) -> dict:
    """
    通用聚合引擎：根据 RuntimeSchema 自适应聚合。

    策略:
      - GROUP BY:   第一个日期列 (按日截断)；无日期则全局聚合
      - METRICS:    所有数值列 → SUM（排除 ID 列）
      - DIMENSIONS: 分类列的 COUNT DISTINCT（≤10 列, 每列 ≤100 类别）

    Returns:
        {"result": DataFrame, "summary": dict}，供 report 层使用
    """
    if df.empty:
        return {"result": pd.DataFrame(), "summary": {"message": "无数据"}}

    # 日期维度
    date_col = None
    for dc in schema.date_columns:
        if dc in df.columns:
            date_col = dc
            break

    # 指标列
    metric_cols = [c for c in schema.numeric_columns if c in df.columns and c not in schema.id_columns]
    if not metric_cols and schema.numeric_columns:
        metric_cols = [c for c in schema.numeric_columns if c in df.columns][:10]
    if not metric_cols:
        # 桌面兜底：所有数值类型列
        metric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c not in schema.id_columns]

    # 构建聚合
    if date_col and date_col in df.columns:
        col_series = df[date_col]
        if pd.api.types.is_datetime64_any_dtype(col_series):
            df["_stat_date"] = col_series.dt.date
        else:
            df["_stat_date"] = pd.to_datetime(col_series, errors="coerce").dt.date
        group_cols = ["_stat_date"]
    else:
        group_cols = []
        logger.info("无日期列，执行全局聚合")

    agg_spec = {}
    for mc in metric_cols[:15]:  # 最多 15 个指标
        agg_spec[f"{mc}_sum"] = (mc, "sum")
        agg_spec[f"{mc}_avg"] = (mc, "mean")

    if not agg_spec:
        logger.warning("无可用数值列，跳过聚合")
        return {"result": df.head(1).assign(_info="无可用指标") if not df.empty else pd.DataFrame(),
                "summary": {"message": "无可用数值列"}}

    result = df.groupby(group_cols, dropna=False).agg(**agg_spec).reset_index() if group_cols else pd.DataFrame({
        k: [df[col].sum()] if "_sum" in k else [df[col].mean()] if "_avg" in k else [0]
        for k, (col, _) in agg_spec.items()
    })

    result = result.round(4)
    n_dates = result["_stat_date"].nunique() if "_stat_date" in result.columns else 1
    total_rows = len(result)
    logger.info("自适应聚合: %d 日期 × %d 指标 = %d 行", n_dates, len(agg_spec), total_rows)

    summary = {
        "聚合维度": f"{n_dates} 个日期" if date_col else "全局(无日期)",
        "数值指标": [f"{mc}_sum" for mc in metric_cols[:15]],
        "聚合行数": total_rows,
    }

    return {"result": result, "summary": summary}


def run_aggregate_auto(schema_yaml: str = None) -> dict:
    """
    Step 4 自动模式入口：DWD → DWS（零配置，SQL 直读）

    从 DWD 表读取 → profile → 自适应聚合 → 写回 DWS 表
    """
    start = datetime.now()
    try:
        engine = get_engine()
        df_dwd = pd.read_sql("SELECT * FROM dwd_orders", engine)
        if df_dwd.empty:
            return {
                "task": "auto_aggregate",
                "status": "skipped",
                "reason": "DWD 表为空",
                "duration": (datetime.now() - start).total_seconds(),
            }

        inferred = profile_dataframe(df_dwd)
        yaml_raw = load_yaml_schema(schema_yaml)
        if yaml_raw:
            yaml_schema = yaml_to_runtime_schema(yaml_raw)
            schema = merge_schemas(yaml_schema, inferred)
        else:
            schema = inferred

        agg_result = auto_aggregate(df_dwd, schema)
        result_df = agg_result["result"]

        if not result_df.empty and "_info" not in str(result_df.columns):
            dynamic_load("dws_sales_daily", result_df)

        return {
            "task": "auto_aggregate",
            "status": "success",
            "rows": len(result_df),
            "summary": agg_result["summary"],
            "duration": (datetime.now() - start).total_seconds(),
        }
    except Exception as e:
        logger.error("auto_aggregate 失败: %s", e, exc_info=True)
        return {
            "task": "auto_aggregate",
            "status": "failed",
            "error": str(e),
            "duration": (datetime.now() - start).total_seconds(),
        }


