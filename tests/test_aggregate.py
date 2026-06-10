"""
aggregate 模块单元测试
"""

import pandas as pd
import pytest
from etl.aggregate import aggregate_daily


@pytest.fixture
def sample_dwd_data():
    """构造 DWD 层样例数据"""
    return pd.DataFrame(
        {
            "stat_date": [
                "2024-01-01", "2024-01-01", "2024-01-01",
                "2024-01-02", "2024-01-02",
                "2024-01-03",
            ],
            "invoice_no": [
                "INV001", "INV002", "INV003",
                "INV004", "INV005",
                "INV006",
            ],
            "customer_id": [
                "C001", "C002", "C001",
                "C003", "C001",
                "C004",
            ],
            "order_amount": [
                100.0, 200.0, 50.0,
                300.0, 100.0,
                500.0,
            ],
        }
    )


class TestAggregateDaily:
    """日度聚合测试"""

    def test_aggregate_correctly(self, sample_dwd_data):
        df = aggregate_daily(sample_dwd_data)

        # 检查日期数量
        assert len(df) == 3

        # 第一日：3 订单, 2 客户 (C001 C002), 金额 350
        day1 = df[df["stat_date"] == "2024-01-01"].iloc[0]
        assert day1["daily_order_count"] == 3
        assert day1["daily_customer_count"] == 2
        assert day1["daily_sales_amount"] == 350.0
        assert day1["daily_avg_order_amount"] == pytest.approx(350.0 / 3)

    def test_order_count_is_unique(self, sample_dwd_data):
        """测试订单数使用去重计数"""
        # 添加重复订单
        dup = pd.DataFrame(
            {
                "stat_date": ["2024-01-01"],
                "invoice_no": ["INV001"],  # 重复
                "customer_id": ["C001"],
                "order_amount": [100.0],
            }
        )
        df_all = pd.concat([sample_dwd_data, dup], ignore_index=True)
        df = aggregate_daily(df_all)

        day1 = df[df["stat_date"] == "2024-01-01"].iloc[0]
        # 订单数为去重计数，仍为 3
        assert day1["daily_order_count"] == 3

    def test_avg_order_amount_formula(self, sample_dwd_data):
        """测试客单价 = 销售额 / 订单数"""
        df = aggregate_daily(sample_dwd_data)
        for _, row in df.iterrows():
            expected = row["daily_sales_amount"] / row["daily_order_count"]
            assert row["daily_avg_order_amount"] == pytest.approx(expected)
