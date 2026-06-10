"""
report 模块单元测试
"""

import os
import tempfile
import pandas as pd
import pytest
from etl.report import generate_summary, save_report


@pytest.fixture
def sample_dws_data():
    """构造 DWS 层样例数据"""
    return pd.DataFrame(
        {
            "stat_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "daily_order_count": [100, 150, 120],
            "daily_customer_count": [80, 100, 90],
            "daily_sales_amount": [5000.0, 7500.0, 6000.0],
            "daily_avg_order_amount": [50.0, 50.0, 50.0],
        }
    )


class TestGenerateSummary:
    """日报汇总测试"""

    def test_summary_keys(self, sample_dws_data):
        summary = generate_summary(sample_dws_data)
        expected_keys = [
            "统计天数", "累计销售额", "累计订单数", "累计客户数",
            "日均销售额", "平均客单价", "最高单日销售额", "最高单日订单数",
        ]
        for key in expected_keys:
            assert key in summary

    def test_summary_values(self, sample_dws_data):
        summary = generate_summary(sample_dws_data)
        assert summary["统计天数"] == 3
        assert summary["累计销售额"] == pytest.approx(18500.0)
        assert summary["累计订单数"] == 370
        assert summary["最高单日销售额"] == pytest.approx(7500.0)

    def test_empty_data(self):
        df = pd.DataFrame(
            columns=[
                "stat_date", "daily_order_count",
                "daily_customer_count", "daily_sales_amount",
                "daily_avg_order_amount",
            ]
        )
        summary = generate_summary(df)
        assert summary == {"message": "无数据"}


class TestSaveReport:
    """日报保存测试"""

    def test_saves_csv_file(self, sample_dws_data):
        summary = generate_summary(sample_dws_data)
        with tempfile.TemporaryDirectory() as tmpdir:
            path = save_report(sample_dws_data, summary, tmpdir)
            assert os.path.exists(path)
            assert path.endswith(".csv")

            # 验证文件内容
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            assert "订单数据日报" in content
