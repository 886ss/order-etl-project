"""
transform 模块单元测试
"""

import pandas as pd
import pytest
from etl.transform import clean_data


@pytest.fixture
def sample_ods_data():
    """构造 ODS 层样例数据"""
    return pd.DataFrame(
        {
            "invoice_no": [
                "INV001", "INV002", "C-INV003",
                "INV004", "INV005", "INV006",
            ],
            "stock_code": [
                "SKU01", "SKU02", "SKU03",
                "SKU04", "SKU05", "SKU06",
            ],
            "description": [
                "Item A", "Item B", "Refund C",
                "Item D", "Item E", "Item F",
            ],
            "quantity": [5, 3, 2, -1, 0, 10],
            "invoice_date": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-03",
                 "2024-01-04", "2024-01-05", "2024-01-06"]
            ),
            "unit_price": [10.0, 5.0, 15.0, 20.0, 8.0, 0.0],
            "customer_id": ["C001", "C002", "C003", "C004", None, "C005"],
            "country": [
                "UK", "UK", "UK", "UK", "UK", "UK",
            ],
        }
    )


class TestCleanData:
    """数据清洗测试"""

    def test_removes_cancelled_orders(self, sample_ods_data):
        """测试过滤取消订单（InvoiceNo 以 C 开头）"""
        df, _ = clean_data(sample_ods_data.copy())
        assert "C-INV003" not in df["invoice_no"].values

    def test_removes_null_customer_id(self, sample_ods_data):
        """测试去除 CustomerID 为空的记录"""
        df, _ = clean_data(sample_ods_data.copy())
        assert df["customer_id"].isna().sum() == 0

    def test_removes_non_positive_quantity(self, sample_ods_data):
        """测试去除 Quantity ≤ 0 的记录"""
        df, _ = clean_data(sample_ods_data.copy())
        assert (df["quantity"] <= 0).sum() == 0

    def test_removes_non_positive_price(self, sample_ods_data):
        """测试去除 UnitPrice ≤ 0 的记录"""
        df, _ = clean_data(sample_ods_data.copy())
        assert (df["unit_price"] <= 0).sum() == 0

    def test_calculates_order_amount(self, sample_ods_data):
        """测试订单金额计算"""
        df, _ = clean_data(sample_ods_data.copy())
        assert "order_amount" in df.columns
        for _, row in df.iterrows():
            assert row["order_amount"] == row["quantity"] * row["unit_price"]

    def test_adds_etl_time(self, sample_ods_data):
        """测试 ETL 时间戳添加"""
        df, _ = clean_data(sample_ods_data.copy())
        assert "etl_time" in df.columns
        assert df["etl_time"].notna().all()

    def test_returns_empty_for_all_bad_data(self):
        """测试全量脏数据返回空 DataFrame"""
        bad_data = pd.DataFrame(
            {
                "invoice_no": ["C-001", "C-002"],
                "stock_code": ["SKU01", "SKU02"],
                "description": ["Bad A", "Bad B"],
                "quantity": [-1, -2],
                "invoice_date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "unit_price": [-5.0, -10.0],
                "customer_id": [None, None],
                "country": ["UK", "UK"],
            }
        )
        df, _ = clean_data(bad_data)
        assert len(df) == 0

    def test_rejected_df_has_reason_column(self, sample_ods_data):
        """脏数据归档包含 rejected_reason 列"""
        _, rejected = clean_data(sample_ods_data.copy())
        # C-INV003(取消) + INV005(null cust先捕获) + INV004(qty=-1) + INV006(price=0) = 4 行
        # 注意: INV005 同时有 null customer_id 和 qty=0，但 null cust 先匹配，只记录一次
        assert "rejected_reason" in rejected.columns
        assert len(rejected) == 4

    def test_rejected_df_contains_cancelled(self, sample_ods_data):
        """取消订单被归档且原因正确"""
        _, rejected = clean_data(sample_ods_data.copy())
        cancelled = rejected[rejected["rejected_reason"] == "取消订单 (InvoiceNo以C开头)"]
        assert len(cancelled) == 1
        assert cancelled.iloc[0]["invoice_no"] == "C-INV003"
