"""
quality 模块单元测试

使用 SQLite 内存数据库模拟质量检查逻辑。
"""

import pytest
from sqlalchemy import create_engine, text
from etl.quality import check_null_values, check_duplicates, check_abnormal_amounts


@pytest.fixture
def engine_with_data():
    """创建 SQLite 内存数据库并插入测试数据"""
    engine = create_engine("sqlite:///:memory:")

    # 建表 (SQLAlchemy 2.0: 使用 begin() 获取连接)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
            CREATE TABLE ods_orders (
                invoice_no   VARCHAR,
                stock_code   VARCHAR,
                description  TEXT,
                quantity     INTEGER,
                invoice_date TIMESTAMP,
                unit_price   NUMERIC,
                customer_id  VARCHAR,
                country      VARCHAR
            )
            """
            )
        )

    # 插入测试数据
    test_data = [
        ("INV001", "SKU001", "Item A", 5, "2024-01-01", 10.0, "C001", "UK"),
        ("INV001", "SKU002", "Item B", 3, "2024-01-01", 5.0, "C001", "UK"),
        ("INV002", "SKU003", "Item C", -2, "2024-01-02", 15.0, "C002", "UK"),
        ("INV003", "SKU004", "Item D", 1, "2024-01-02", -5.0, "C003", "UK"),
        (None, "SKU005", "Item E", 2, "2024-01-03", 20.0, "C004", "UK"),
        ("INV004", "SKU006", "Item F", 0, "2024-01-03", 10.0, None, "UK"),
    ]

    with engine.begin() as conn:
        for row in test_data:
            conn.execute(
                text(
                    "INSERT INTO ods_orders VALUES "
                    "(:inv, :sku, :desc, :qty, :dt, :price, :cust, :ctry)"
                ),
                {
                    "inv": row[0],
                    "sku": row[1],
                    "desc": row[2],
                    "qty": row[3],
                    "dt": row[4],
                    "price": row[5],
                    "cust": row[6],
                    "ctry": row[7],
                },
            )

    return engine


class TestNullCheck:
    """空值检查测试"""

    def test_null_detection(self, engine_with_data):
        result = check_null_values(engine_with_data)
        assert result["total_rows"] == 6
        assert result["null_invoice_no"] == 1  # INV001 missing
        assert result["null_customer_id"] == 1  # INV004 missing customer


class TestDuplicateCheck:
    """重复检查测试"""

    def test_duplicate_detection(self, engine_with_data):
        result = check_duplicates(engine_with_data)
        assert result["total_rows"] == 6
        assert result["unique_invoice_no"] == 4  # None counted as NULL
        assert result["duplicate_invoice_count"] == 2  # 6 - 4


class TestAbnormalCheck:
    """异常金额检查测试"""

    def test_abnormal_detection(self, engine_with_data):
        result = check_abnormal_amounts(engine_with_data)
        assert result["negative_quantity_count"] >= 1  # -2
        assert result["negative_price_count"] >= 1  # -5.0
        assert result["zero_quantity_count"] >= 1  # 0
