"""
quality 模块单元测试

使用 SQLite 内存数据库模拟质量检查逻辑。
"""

import pytest
from sqlalchemy import create_engine, text
from etl.quality import check_null_values, check_duplicates, check_abnormal_amounts, check_null_rate_warnings


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


@pytest.fixture
def engine_with_null_rates():
    """带非关键字段空值的数据，用于测试空值率阈值"""
    import sqlalchemy as sa
    engine = sa.create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        conn.execute(
            text("""
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
            """)
        )

    # 10 行数据：3 行 description 为空，5 行 country 为空
    data = [
        ("INV001", "SKU01", "Item A",  1, "2024-01-01", 10.0, "C01", "UK"),
        ("INV002", "SKU02", None,      2, "2024-01-02", 20.0, "C02", None),
        ("INV003", "SKU03", "Item C",  3, "2024-01-03", 30.0, "C03", None),
        ("INV004", "SKU04", None,      4, "2024-01-04", 40.0, "C04", None),
        ("INV005", "SKU05", "Item E",  5, "2024-01-05", 50.0, "C05", "UK"),
        ("INV006", "SKU06", "Item F",  6, "2024-01-06", 60.0, "C06", None),
        ("INV007", "SKU07", None,      7, "2024-01-07", 70.0, "C07", "UK"),
        ("INV008", "SKU08", "Item H",  8, "2024-01-08", 80.0, "C08", None),
        ("INV009", "SKU09", "Item I",  9, "2024-01-09", 90.0, "C09", "UK"),
        ("INV010", "SKU10", "Item J", 10, "2024-01-10",100.0, "C10", "UK"),
    ]

    with engine.begin() as conn:
        for row in data:
            conn.execute(
                text(
                    "INSERT INTO ods_orders VALUES "
                    "(:inv, :sku, :desc, :qty, :dt, :price, :cust, :ctry)"
                ),
                {
                    "inv": row[0], "sku": row[1], "desc": row[2],
                    "qty": row[3], "dt": row[4], "price": row[5],
                    "cust": row[6], "ctry": row[7],
                },
            )

    return engine


class TestNullRateWarnings:
    """空值率阈值预警测试"""

    def test_no_warning_below_threshold(self, engine_with_data):
        """默认阈 30%，6 行无空值 → 无预警"""
        result = check_null_rate_warnings(engine_with_data)
        assert result["total_rows"] == 6
        assert len(result["warnings"]) == 0

    def test_warning_above_threshold(self, engine_with_null_rates):
        """10 行中 country 空值率 50% > 阈值 30% → 触发预警
        description 空值率 30% ≤ 阈值 30% → 不触发
        """
        result = check_null_rate_warnings(engine_with_null_rates, threshold=0.30)
        assert result["total_rows"] == 10
        warned_fields = {w["field"] for w in result["warnings"]}
        # country: 5/10 = 50% > 30% → 预警
        assert "country" in warned_fields
        # description: 3/10 = 30% ≤ 30% → 不预警
        assert "description" not in warned_fields

    def test_custom_threshold(self, engine_with_null_rates):
        """自定义阈 0.10：description 30% 和 country 50% 都应触发"""
        result = check_null_rate_warnings(engine_with_null_rates, threshold=0.10)
        warned_fields = {w["field"] for w in result["warnings"]}
        assert "description" in warned_fields
        assert "country" in warned_fields

    def test_all_fields_within_threshold(self, engine_with_null_rates):
        """阈 0.80：所有字段空值率 ≤ 80% → 无预警"""
        result = check_null_rate_warnings(engine_with_null_rates, threshold=0.80)
        assert len(result["warnings"]) == 0
