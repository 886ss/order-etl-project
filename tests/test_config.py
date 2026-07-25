"""
config 模块单元测试
"""

import json
import os
import tempfile

import pytest

from etl.config import (
    RuntimeSchema,
    load_yaml_schema,
    yaml_to_runtime_schema,
    load_cache,
    save_cache,
    merge_schemas,
)


class TestRuntimeSchema:
    def test_default_values(self):
        s = RuntimeSchema()
        assert s.source_mapping == {}
        assert s.date_columns == []
        assert s.numeric_columns == []
        assert s.quality_null_threshold == 0.30

    def test_field_assignment(self):
        s = RuntimeSchema(
            source_mapping={"A": "a"},
            date_columns=["dt"],
            numeric_columns=["val"],
        )
        assert s.source_mapping["A"] == "a"
        assert s.date_columns == ["dt"]


class TestLoadYamlSchema:
    def test_load_existing(self):
        """etl/schema.yaml 应存在且可解析"""
        raw = load_yaml_schema("etl/schema.yaml")
        assert raw is not None
        assert "source_column_mapping" in raw
        assert "cleaning" in raw
        assert "quality" in raw

    def test_load_nonexistent(self):
        raw = load_yaml_schema("nonexistent_file.yaml")
        assert raw is None

    def test_yaml_to_runtime_schema(self):
        raw = load_yaml_schema("etl/schema.yaml")
        s = yaml_to_runtime_schema(raw)
        assert len(s.source_mapping) == 8
        assert s.source_mapping["InvoiceNo"] == "invoice_no"
        assert s.source_mapping["CustomerID"] == "customer_id"
        assert len(s.cancel_rules) == 1
        assert s.cancel_rules[0]["method"] == "string_startswith"
        assert len(s.computed_columns) == 1
        assert s.computed_columns[0]["expression"] == "quantity * unit_price"


class TestCache:
    def test_roundtrip(self):
        s = RuntimeSchema(
            source_mapping={"X": "x"},
            date_columns=["dt"],
            numeric_columns=["n1", "n2"],
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "cache.json")
            save_cache(s, path)
            data = load_cache(path)
            assert data is not None
            assert data["source_mapping"] == {"X": "x"}
            assert data["date_columns"] == ["dt"]

    def test_load_nonexistent_cache(self):
        data = load_cache("nonexistent_cache.json")
        assert data is None


class TestMergeSchemas:
    def test_yaml_overrides_inferred(self):
        yaml_schema = RuntimeSchema(
            source_mapping={"A": "a"},
            cancel_rules=[{"method": "test"}],
        )
        inferred = RuntimeSchema(
            source_mapping={},
            date_columns=["dt"],
            numeric_columns=["v1"],
        )
        result = merge_schemas(yaml_schema, inferred)
        # YAML 的映射优先
        assert result.source_mapping == {"A": "a"}
        # YAML 的规则优先
        assert result.cancel_rules == [{"method": "test"}]
        # 推断的列角色保留
        assert result.date_columns == ["dt"]
        assert result.numeric_columns == ["v1"]

    def test_empty_yaml_returns_inferred(self):
        inferred = RuntimeSchema(date_columns=["dt"])
        result = merge_schemas(RuntimeSchema(), inferred)
        assert result.date_columns == ["dt"]
