"""
端到端测试: 全链路 S→P→Q→T→A→R（无 PostgreSQL 依赖）
数据集: E-Commerce Orders 2026 (30K × 41)
"""

import os
import sys
import tempfile

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from etl.sanitizer import read_csv_safe
from etl.profiler import profile_dataframe, infer_and_clean_dates
from etl.transform import clean_data_auto
from etl.aggregate import auto_aggregate
from etl.report import auto_report
from etl.config import RuntimeSchema, load_yaml_schema, yaml_to_runtime_schema, merge_schemas

DATASET = "data/ecommerce_orders_2026.csv"


@pytest.fixture(scope="module")
def df_raw():
    return read_csv_safe(DATASET)


@pytest.fixture(scope="module")
def schema():
    yaml_raw = load_yaml_schema("data/schema_2026.yaml")
    yaml_schema = yaml_to_runtime_schema(yaml_raw) if yaml_raw else RuntimeSchema()
    df = read_csv_safe(DATASET)
    inferred = profile_dataframe(df)
    return merge_schemas(yaml_schema, inferred) if yaml_raw else inferred


class TestFullPipeline:
    """全链路集成测试"""

    def test_step1_read(self, df_raw):
        """Step 1: 文件读取"""
        assert len(df_raw) == 30000
        assert len(df_raw.columns) == 41

    def test_step2_profile(self, schema):
        """Step 2: 列推断"""
        assert len(schema.date_columns) >= 1
        assert "Order_Date" in schema.date_columns
        assert len(schema.numeric_columns) >= 10
        assert "Order_ID" in schema.id_columns

    def test_step3_clean(self, df_raw, schema):
        """Step 3: 通用清洗 — BUG-002 修复验证"""
        df = infer_and_clean_dates(df_raw, schema.date_columns)
        # BUG-002: Order_ID is int64 — must NOT crash
        df["Order_ID"] = df["Order_ID"].astype(int)  # restore int
        df_clean, rejected = clean_data_auto(df, schema)
        assert len(df_clean) > 0
        assert "etl_time" in df_clean.columns
        # 合成数据集很干净，拒绝行应很少
        assert len(rejected) < len(df_raw) * 0.1

    def test_step4_aggregate(self, df_raw, schema):
        """Step 4: 自适应聚合"""
        df = infer_and_clean_dates(df_raw, schema.date_columns)
        df["Order_ID"] = df["Order_ID"].astype(int)
        df_clean, _ = clean_data_auto(df, schema)
        result = auto_aggregate(df_clean, schema)
        assert "result" in result
        assert len(result["result"]) > 0

    def test_step5_report(self, df_raw, schema):
        """Step 5: 报告生成"""
        df = infer_and_clean_dates(df_raw, schema.date_columns)
        df["Order_ID"] = df["Order_ID"].astype(int)
        df_clean, _ = clean_data_auto(df, schema)
        agg = auto_aggregate(df_clean, schema)
        with tempfile.TemporaryDirectory() as tmp:
            path = auto_report(agg["result"], agg["summary"], tmp)
            assert os.path.exists(path)
            # 检查 CSV 和 JSON 都生成
            assert any(f.endswith(".csv") for f in os.listdir(tmp))
            assert any(f.endswith(".json") for f in os.listdir(tmp))

    def test_amount_columns_detected(self, schema):
        """数值列应包含关键金额字段"""
        numeric_set = set(schema.numeric_columns)
        assert "Order_Amount" in numeric_set
        assert "Unit_Price" in numeric_set
        assert "Quantity" in numeric_set


class TestWithYamlOverride:
    """YAML schema 覆盖测试"""

    def test_uci_yaml_produces_expected_schema(self):
        """UCI schema.yaml 应生成与旧硬编码等效的配置"""
        raw = load_yaml_schema("etl/schema.yaml")
        s = yaml_to_runtime_schema(raw)
        assert s.source_mapping["InvoiceNo"] == "invoice_no"
        assert s.source_mapping["CustomerID"] == "customer_id"
        assert len(s.cancel_rules) == 1
        assert s.cancel_rules[0]["value"] == "C"

    def test_2026_yaml_overrides_auto(self):
        """2026 schema.yaml 覆盖自动推断"""
        yaml_raw = load_yaml_schema("data/schema_2026.yaml")
        yaml_schema = yaml_to_runtime_schema(yaml_raw)
        df = read_csv_safe(DATASET)
        inferred = profile_dataframe(df)
        merged = merge_schemas(yaml_schema, inferred)
        # YAML 的映射优先
        assert merged.source_mapping["Order_ID"] == "invoice_no"
        # 推断的列角色保留
        assert "Order_Date" in merged.date_columns


class TestBugFixes:
    """关键 Bug 修复回归测试"""

    def test_bug002_int_column_no_crash(self, df_raw, schema):
        """BUG-002: int64 列上调用 clean_data_auto 不崩溃"""
        df = df_raw.copy()
        df["Order_ID"] = df["Order_ID"].astype(int)
        # 不应抛异常
        df_clean, rejected = clean_data_auto(df, schema)
        assert len(df_clean) > 0

    def test_bug001_schema_based_mapping(self):
        """BUG-001: 列映射现在由 schema 驱动而非硬编码"""
        yaml_raw = load_yaml_schema("data/schema_2026.yaml")
        s = yaml_to_runtime_schema(yaml_raw)
        assert len(s.source_mapping) == 8
        assert s.source_mapping.get("Order_ID") == "invoice_no"

    def test_bug003_cancel_flexibility(self):
        """BUG-003: 取消检测规则可配置而非硬编码 C 前缀"""
        # UCI schema: 有 cancel_rules
        uci = yaml_to_runtime_schema(load_yaml_schema("etl/schema.yaml"))
        assert len(uci.cancel_rules) == 1
        assert uci.cancel_rules[0]["method"] == "string_startswith"

        # 2026 schema: 无 cancel_rules（空列表，非 None）
        s2026 = yaml_to_runtime_schema(load_yaml_schema("data/schema_2026.yaml"))
        assert s2026.cancel_rules == []

    def test_bug004_computed_columns_configurable(self):
        """BUG-004: 金额公式可配置"""
        uci = yaml_to_runtime_schema(load_yaml_schema("etl/schema.yaml"))
        assert len(uci.computed_columns) == 1
        assert uci.computed_columns[0]["expression"] == "quantity * unit_price"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
