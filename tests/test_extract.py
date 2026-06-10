"""
extract 模块单元测试
"""

import os
import tempfile
import pandas as pd
import pytest
from etl.extract import read_csv_data, validate_columns, COLUMN_MAPPING


class TestReadCsvData:
    """CSV 数据读取测试"""

    def test_read_valid_csv(self):
        """测试读取有效 CSV 文件"""
        # 创建临时 CSV
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(
                "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,"
                "UnitPrice,CustomerID,Country\n"
                "536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,"
                "2010-12-01 08:26:00,2.55,17850,United Kingdom\n"
            )
            temp_path = f.name

        try:
            df = read_csv_data(temp_path)
            assert len(df) == 1
            assert df.iloc[0]["InvoiceNo"] == "536365"
            assert df.iloc[0]["CustomerID"] == "17850"
        finally:
            os.unlink(temp_path)

    def test_read_nonexistent_file(self):
        """测试读取不存在的文件"""
        with pytest.raises(FileNotFoundError):
            read_csv_data("nonexistent_file.csv")


class TestValidateColumns:
    """列校验测试"""

    def test_all_columns_present(self):
        """测试所有列都存在"""
        df = pd.DataFrame(columns=list(COLUMN_MAPPING.keys()))
        assert validate_columns(df) is True

    def test_missing_column_raises_error(self):
        """测试缺少列时抛出异常"""
        df = pd.DataFrame(columns=["InvoiceNo", "StockCode"])
        with pytest.raises(ValueError, match="CSV 缺少列"):
            validate_columns(df)
