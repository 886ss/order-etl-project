"""
db 模块单元测试

使用 SQLite 内存库验证 truncate_and_load 的原子事务行为。
"""

from datetime import date
import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from etl.db import truncate_and_load, append_to_table, ODS_DTYPE


@pytest.fixture
def sqlite_engine():
    """创建 SQLite 内存库，模拟 ods_orders 表结构"""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """CREATE TABLE ods_orders (
                    invoice_no   VARCHAR(50),
                    stock_code   VARCHAR(50),
                    description  TEXT,
                    quantity     INTEGER,
                    invoice_date TIMESTAMP,
                    unit_price   NUMERIC(10, 4),
                    customer_id  VARCHAR(50),
                    country      VARCHAR(100)
                )"""
            )
        )
    return engine


def make_df(rows: list[tuple]) -> pd.DataFrame:
    """构建测试用 DataFrame"""
    columns = [
        "invoice_no", "stock_code", "description", "quantity",
        "invoice_date", "unit_price", "customer_id", "country",
    ]
    return pd.DataFrame(rows, columns=columns)


class TestTruncateAndLoad:
    """truncate_and_load 原子事务测试"""

    def test_basic_write(self, sqlite_engine, monkeypatch):
        """写入后行数正确，覆盖旧数据"""
        monkeypatch.setattr("etl.db.get_engine", lambda: sqlite_engine)

        # 先写一批
        df1 = make_df([("INV001", "SKU001", "Item A", 5, date(2024, 1, 1), 10.0, "C001", "UK")])
        count = truncate_and_load("ods_orders", df1, ODS_DTYPE)
        assert count == 1

        # 再写一批（应覆盖）
        df2 = make_df([
            ("INV002", "SKU002", "Item B", 3, date(2024, 1, 2), 5.0, "C002", "UK"),
            ("INV003", "SKU003", "Item C", 1, date(2024, 1, 3), 15.0, "C003", "UK"),
        ])
        count = truncate_and_load("ods_orders", df2, ODS_DTYPE)
        assert count == 2

        # 确认表中只有第二批数据
        with sqlite_engine.connect() as conn:
            rows = conn.execute(text("SELECT invoice_no FROM ods_orders")).fetchall()
        invoices = {r[0] for r in rows}
        assert invoices == {"INV002", "INV003"}

    def test_empty_dataframe(self, sqlite_engine, monkeypatch):
        """空 DataFrame 写入不会抛异常，行数为 0"""
        monkeypatch.setattr("etl.db.get_engine", lambda: sqlite_engine)

        df = make_df([])
        count = truncate_and_load("ods_orders", df, ODS_DTYPE)
        assert count == 0

    def test_nonexistent_table(self, sqlite_engine, monkeypatch):
        """写入不存在的表应抛异常"""
        monkeypatch.setattr("etl.db.get_engine", lambda: sqlite_engine)

        df = make_df([("INV001", "SKU001", "X", 1, date(2024, 1, 1), 1.0, "C001", "UK")])
        with pytest.raises(Exception):
            truncate_and_load("nonexistent_table", df, ODS_DTYPE)


class TestAppendToTable:
    """append_to_table 增量追加测试"""

    def test_append_does_not_clear(self, sqlite_engine, monkeypatch):
        """追加写入不覆盖已有数据"""
        monkeypatch.setattr("etl.db.get_engine", lambda: sqlite_engine)

        # 先写一批
        df1 = make_df([("INV001", "SKU001", "Item A", 5, date(2024, 1, 1), 10.0, "C001", "UK")])
        append_to_table("ods_orders", df1, ODS_DTYPE)

        # 再追加一批——不应清空第一批
        df2 = make_df([("INV002", "SKU002", "Item B", 3, date(2024, 1, 2), 5.0, "C002", "UK")])
        count = append_to_table("ods_orders", df2, ODS_DTYPE)
        assert count == 1

        # 确认两批数据都在
        with sqlite_engine.connect() as conn:
            rows = conn.execute(text("SELECT invoice_no FROM ods_orders")).fetchall()
        invoices = {r[0] for r in rows}
        assert invoices == {"INV001", "INV002"}
