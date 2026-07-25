"""
数据库连接管理模块
==================
提供 PostgreSQL 连接引擎、共享类型映射和通用加载函数。
配置通过环境变量读取，支持默认值。

引擎为模块级单例，全项目共享同一连接池，
避免重复创建连接池导致资源泄漏。
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.types import Numeric, Integer, String, DateTime
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
    "database": os.getenv("PG_DATABASE", "order_warehouse"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}

_engine = None


def get_engine():
    """获取 PostgreSQL 数据库引擎（模块级单例）"""
    global _engine
    if _engine is None:
        url = (
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        _engine = create_engine(
            url,
            echo=False,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,
        )
    return _engine


# ============================================================
# 共享列类型映射 — ODS/DWD 表 to_sql 时使用
# 所有写表操作通过此常量，确保类型一致
# ============================================================
ODS_DTYPE = {
    "invoice_no": String(50),
    "stock_code": String(50),
    "description": String,
    "quantity": Integer,
    "invoice_date": DateTime,
    "unit_price": Numeric(10, 4),
    "customer_id": String(50),
    "country": String(100),
}

DWD_DTYPE = {
    **ODS_DTYPE,
    "order_amount": Numeric(12, 4),
    "etl_time": DateTime,
}

# 脏数据归档表 — 记录清洗过程中被剔除的数据行及其拒绝原因
REJECTED_DTYPE = {
    **ODS_DTYPE,
    "rejected_reason": String(200),
    "rejected_at": DateTime,
}


def append_to_table(table_name: str, df, dtype: dict) -> int:
    """
    追加写入（不做清空），用于增量 ETL 模式。

    写入失败时已有数据不受影响。
    """
    engine = get_engine()
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists="append", index=False, dtype=dtype)
    return len(df)


def upsert_incremental(table_name: str, df, dtype: dict, date_column: str) -> int:
    """
    幂等增量写入：删除目标日期范围内的旧批次，再插入新批次。

    DELETE + INSERT 在同一事务中执行，Airflow 重跑不会产生重复数据。
    适用于 ODS/DWD 层的增量写入。
    生产级参考：ON CONFLICT DO NOTHING（PG）或 batch_id 去重。

    Args:
        table_name: 目标表名
        df: 要写入的 DataFrame
        dtype: SQLAlchemy 列类型映射
        date_column: 日期列名，用于划定批次范围

    Returns:
        写入行数
    """
    engine = get_engine()
    dates = df[date_column].drop_duplicates()
    min_date = dates.min()
    max_date = dates.max()

    with engine.begin() as conn:
        if min_date is not None and max_date is not None:
            conn.execute(
                text(
                    f"DELETE FROM {table_name} "
                    f"WHERE {date_column} >= :min_d AND {date_column} <= :max_d"
                ),
                {"min_d": min_date, "max_d": max_date},
            )
        df.to_sql(table_name, conn, if_exists="append", index=False, dtype=dtype)
    return len(df)


def truncate_and_load(table_name: str, df, dtype: dict) -> int:
    """
    在同一事务中 TRUNCATE + INSERT，保证原子性。

    Args:
        table_name: 目标表名
        df: 要写入的 DataFrame
        dtype: SQLAlchemy 列类型映射

    Returns:
        写入行数
    """
    engine = get_engine()
    with engine.begin() as conn:
        dialect_name = engine.dialect.name
        if dialect_name == "postgresql":
            conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY"))
        else:
            conn.execute(text(f"DELETE FROM {table_name}"))
        df.to_sql(table_name, conn, if_exists="append", index=False, dtype=dtype)
    return len(df)


def dynamic_load(table_name: str, df, mode: str = "replace") -> int:
    """动态建表写入（无预定义 dtype）。auto_* 三步共用此函数。"""
    engine = get_engine()
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists=mode, index=False)
    return len(df)
