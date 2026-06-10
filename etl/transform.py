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
import pandas as pd
from sqlalchemy import text
from etl.db import get_engine


def load_ods_data(engine) -> pd.DataFrame:
    """从 ODS 层读取全量数据"""
    query = "SELECT * FROM ods_orders"
    df = pd.read_sql(query, engine)
    print(f"[transform] 从 ODS 读取: {len(df)} 行")
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
    print(f"[transform] 过滤取消订单: {cancel_count} 行")

    # 2. 去除 CustomerID 为空
    null_cust = df["customer_id"].isna().sum()
    df = df.dropna(subset=["customer_id"])
    print(f"[transform] 去除空客户ID: {null_cust} 行")

    # 3. 去除 Quantity ≤ 0
    neg_qty = (df["quantity"] <= 0).sum()
    df = df[df["quantity"] > 0]
    print(f"[transform] 去除非正数量: {neg_qty} 行")

    # 4. 去除 UnitPrice ≤ 0
    neg_price = (df["unit_price"] <= 0).sum()
    df = df[df["unit_price"] > 0]
    print(f"[transform] 去除非正单价: {neg_price} 行")

    # 5. 计算订单金额
    df["order_amount"] = df["quantity"] * df["unit_price"]

    # 6. 添加 ETL 时间戳
    df["etl_time"] = datetime.now()

    cleaned_count = len(df)
    removed = original_count - cleaned_count
    print(
        f"[transform] 清洗完成: {original_count} → {cleaned_count} 行 "
        f"(剔除 {removed} 行, {removed/original_count*100:.1f}%)"
    )

    return df


def load_to_dwd(df: pd.DataFrame) -> int:
    """
    将清洗后数据写入 DWD 层表 dwd_orders

    Args:
        df: 清洗后的 DataFrame

    Returns:
        写入行数
    """
    engine = get_engine()

    # 只保留 DWD 表需要的列
    dwd_columns = [
        "invoice_no", "stock_code", "description", "quantity",
        "invoice_date", "unit_price", "customer_id", "country",
        "order_amount", "etl_time",
    ]
    df_dwd = df[dwd_columns]

    # 清空 DWD 表后写入（全量同步）
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dwd_orders RESTART IDENTITY"))
        print("[transform] DWD 表已清空")

    df_dwd.to_sql("dwd_orders", engine, if_exists="append", index=False)
    print(f"[transform] DWD 写入完成: {len(df_dwd)} 行")
    return len(df_dwd)


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
        df_dwd = clean_data(df_ods)
        row_count = load_to_dwd(df_dwd)
        return {
            "task": "build_dwd",
            "status": "success",
            "rows": row_count,
            "duration": (datetime.now() - start).total_seconds(),
        }
    except Exception as e:
        return {
            "task": "build_dwd",
            "status": "failed",
            "error": str(e),
            "duration": (datetime.now() - start).total_seconds(),
        }


if __name__ == "__main__":
    result = run_transform()
    print(result)
