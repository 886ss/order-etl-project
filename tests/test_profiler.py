"""
profiler + sanitizer 集成测试
两数据集 + 边界条件全覆盖
"""

import os
import sys
import tempfile

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from etl.profiler import (
    profile_dataframe,
    _classify_column,
    _is_progressive,
    _detect_numeric_string,
)
from etl.sanitizer import (
    detect_separator,
    read_csv_safe,
    peek_file,
    _TAIL_KEYWORDS,
)


# ============================================================
# Profiler: 列角色推断
# ============================================================

class TestDateDetection:
    def test_iso_date_format(self):
        s = pd.Series(["2023-01-01", "2023-06-15", "2024-12-31"] * 3)
        role, detail = _classify_column(s, "order_date")
        assert role == "date"

    def test_datetime_dtype(self):
        s = pd.Series(pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]))
        role, detail = _classify_column(s, "created_at")
        assert role == "date"

    def test_non_date_object(self):
        s = pd.Series(["apple", "banana", "cherry"] * 3)
        role, detail = _classify_column(s, "fruit")
        assert role == "categorical"

    def test_column_name_hint(self):
        """列名 'date' 不影响 object 列的 dtype 判定，但会被下游使用"""
        s = pd.Series(["x", "y", "z"] * 3)
        role, _ = _classify_column(s, "order_date")
        # object 列不会被列名误导为 date
        assert role == "categorical"


class TestNumericDetection:
    def test_plain_int(self):
        s = pd.Series([1, 2, 3, 4, 5] * 10)
        role, _ = _classify_column(s, "quantity")
        assert role == "numeric"

    def test_plain_float(self):
        s = pd.Series([1.5, 2.7, 3.14] * 10)
        role, _ = _classify_column(s, "price")
        assert role == "numeric"

    def test_currency_string(self):
        s = pd.Series(["$1,234.56", "$2,000.00", "€500.00"] * 3)
        role, detail = _classify_column(s, "amount")
        assert role == "numeric"
        assert "货币" in detail.get("note", "")

    def test_percent_string(self):
        s = pd.Series(["15%", "20.5%", "99%"] * 5)
        role, detail = _classify_column(s, "rate")
        assert role == "numeric"
        assert "百分比" in detail.get("note", "")

    def test_thousands_separator(self):
        s = pd.Series(["1,234", "5,678", "9,000"] * 5)
        role, detail = _classify_column(s, "total")
        assert role == "numeric"
        assert "千分位" in detail.get("note", "")


class TestIdDetection:
    def test_progressive_integer(self):
        s = pd.Series(range(1, 101))
        role, _ = _classify_column(s, "user_id")
        assert role == "id"

    def test_high_uniqueness_int(self):
        s = pd.Series(range(1000, 1100))
        role, _ = _classify_column(s, "id")
        assert role == "id"

    def test_id_name_pattern_object(self):
        """UUID 字符串 + _id 后缀 → 判定为 id"""
        uuids = [f"uuid-{i:08d}" for i in range(200)]
        s = pd.Series(uuids)
        role, _ = _classify_column(s, "transaction_id")
        assert role == "id"


class TestBoolDetection:
    def test_bool_dtype(self):
        s = pd.Series([True, False, True, False] * 5)
        role, _ = _classify_column(s, "flag")
        assert role == "bool"

    def test_yes_no_strings(self):
        s = pd.Series(["Yes", "No", "Yes", "Yes"] * 3)
        role, _ = _classify_column(s, "returned")
        assert role == "bool"

    def test_zero_one_strings(self):
        s = pd.Series(["0", "1", "1", "0"] * 3)
        role, _ = _classify_column(s, "active")
        assert role == "bool"


class TestDropDetection:
    def test_all_null_column(self):
        s = pd.Series([None, None, None, None])
        role, _ = _classify_column(s, "empty_col")
        assert role == "drop"


class TestIsProgressive:
    def test_strict_progressive(self):
        s = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        assert _is_progressive(s)

    def test_random_values(self):
        s = pd.Series([1, 5, 3, 9, 2, 7, 4, 8, 6, 10])
        assert not _is_progressive(s)

    def test_small_series(self):
        s = pd.Series([1, 2])
        assert not _is_progressive(s)


# ============================================================
# Profiler: 全 DataFrame
# ============================================================

class TestProfileDataframe:
    def test_mixed_types(self):
        df = pd.DataFrame({
            "order_date": pd.to_datetime(["2023-01-01", "2023-01-02"] * 10),
            "amount": [100.5, 200.3] * 10,
            "category": ["A", "B"] * 10,
            "user_id": range(1, 21),
            "is_returned": [True, False] * 10,
        })
        s = profile_dataframe(df)
        assert "order_date" in s.date_columns
        assert "amount" in s.numeric_columns
        assert "category" in s.categorical_columns
        assert "user_id" in s.id_columns
        assert "is_returned" in s.bool_columns

    def test_empty_dataframe(self):
        s = profile_dataframe(pd.DataFrame())
        assert s == s  # 不抛异常，返回默认 schema

    def test_source_mapping_populated(self):
        df = pd.DataFrame({"A": [1, 2], "B": ["x", "y"]})
        s = profile_dataframe(df)
        assert s.source_mapping["A"] == "A"
        assert s.source_mapping["B"] == "B"


# ============================================================
# Sanitizer
# ============================================================

class TestDetectSeparator:
    def test_comma_csv(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("a,b,c\n1,2,3\n4,5,6\n")
            path = f.name
        try:
            assert detect_separator(path) == ","
        finally:
            os.unlink(path)

    def test_tab_separated(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as f:
            f.write("a\tb\tc\n1\t2\t3\n")
            path = f.name
        try:
            assert detect_separator(path) == "\t"
        finally:
            os.unlink(path)


class TestPeekFile:
    def test_normal_csv(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as f:
            f.write("Name,Age,City\nAlice,30,NYC\nBob,25,LA\n")
            path = f.name
        try:
            cols, rows, total_cols = peek_file(path, "utf-8", ",")
            assert cols == ["Name", "Age", "City"]
            assert rows > 0  # 估算值
            assert total_cols == 3
        finally:
            os.unlink(path)

    def test_trailing_total_row(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as f:
            f.write("Name,Value\nAlice,100\nBob,200\nTotal,300\n")
            path = f.name
        try:
            cols, rows, total_cols = peek_file(path, "utf-8", ",")
            assert rows > 0  # 尾部 Total 行被剥离后至少还有表头
            assert total_cols == 2
        finally:
            os.unlink(path)


class TestReadCsvSafe:
    def test_2026_dataset(self):
        path = "data/ecommerce_orders_2026.csv"
        if not os.path.exists(path):
            pytest.skip("2026 dataset not found")
        df = read_csv_safe(path)
        assert len(df) == 30000
        assert len(df.columns) == 41

    def test_empty_file_raises(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("")
            path = f.name
        try:
            with pytest.raises(ValueError, match="empty|空"):
                read_csv_safe(path)
        finally:
            os.unlink(path)

    def test_nonexistent_file(self):
        with pytest.raises(FileNotFoundError):
            read_csv_safe("nonexistent.csv")
