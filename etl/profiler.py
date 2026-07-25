"""
列角色推断引擎 (Profiler)
=========================
基于列名模式 + dtype + 统计特征的三重推断，零领域假设。

推断流程 (每列):
  1. dtype 快速分类 (int/float/datetime/bool/object)
  2. object 列二次探测 (日期? 数值含符号? 百分比? 纯分类?)
  3. 角色分配 + 异常标记

输出: RuntimeSchema 数据类
"""

from __future__ import annotations

import logging
import re
import warnings
from datetime import datetime

import numpy as np
import pandas as pd
from pandas.api.types import is_bool_dtype, is_numeric_dtype, is_datetime64_any_dtype

from etl.config import RuntimeSchema

logger = logging.getLogger(__name__)

# 工业哨兵值（表示缺失/错误而非真实数据）
_SENTINEL_VALUES = {-9999, -999, -99, 9999, 99999, 999, 32767, -32768}

# 日期列名模式（只匹配明确的日期关键词，避免误判）
_DATE_NAME_PATTERN = re.compile(
    r"(^date$|_date$|^dt$|_dt$|_time$|^time$|timestamp|created_at|updated_at"
    r"|^day$|_day$|^month$|_month$|^year$|_year$)",
    re.IGNORECASE,
)

# ID 列名模式
_ID_NAME_PATTERN = re.compile(
    r"(^id$|_id$|_key$|_pk$|^pk$|uuid|guid|identifier|serial)",
    re.IGNORECASE,
)


def profile_dataframe(df: pd.DataFrame, sample_size: int = 5000) -> RuntimeSchema:
    """
    主入口：分析 DataFrame，返回 RuntimeSchema。

    采样策略：≤sample_size 全量分析，>sample_size 随机采样以避免 O(n²) 开销。
    """
    total = len(df)
    if total == 0:
        logger.warning("空 DataFrame，返回默认 schema")
        return RuntimeSchema()

    sample = df.sample(min(total, sample_size)) if total > sample_size else df
    if total > sample_size:
        logger.info("采样分析: %d / %d 行", sample_size, total)

    schema = RuntimeSchema()
    schema.date_columns = []
    schema.numeric_columns = []
    schema.categorical_columns = []
    schema.id_columns = []
    schema.bool_columns = []
    schema.drop_columns = []

    for col in df.columns:
        role, detail = _classify_column(sample[col], col)
        _assign_role(schema, col, role, detail)

    # 统计
    logger.info(
        "列角色推断: date=%d num=%d cat=%d id=%d bool=%d drop=%d / total=%d",
        len(schema.date_columns), len(schema.numeric_columns),
        len(schema.categorical_columns), len(schema.id_columns),
        len(schema.bool_columns), len(schema.drop_columns), len(df.columns),
    )

    # 构建 source_mapping（列名自映射）
    for col in df.columns:
        if col not in schema.drop_columns:
            schema.source_mapping[col] = col

    return schema


def _classify_column(series: pd.Series, name: str) -> tuple[str, dict]:
    """
    单列分类 → (role, detail_info)
    role ∈ {date, numeric, categorical, id, bool, drop}
    """
    n = len(series)
    null_rate = series.isna().mean()

    # --- 全空列 → drop ---
    if null_rate == 1.0:
        return "drop", {"reason": "全为空值"}

    # --- 布尔列 ---
    if is_bool_dtype(series):
        return "bool", {"null_rate": null_rate}

    # --- 日期时间列 ---
    if is_datetime64_any_dtype(series):
        return "date", {"null_rate": null_rate, "method": "dtype"}

    # --- 数值列 ---
    if is_numeric_dtype(series):
        return _classify_numeric(series, name, n, null_rate)

    # --- object/string 列 → 二次探测 ---
    return _classify_object(series, name, n, null_rate)


# ============================================================
# 数值列分类
# ============================================================

def _classify_numeric(series: pd.Series, name: str, n: int, null_rate: float) -> tuple[str, dict]:
    """对已确认是数值类型的列进行角色分配"""
    valid = series.dropna()
    unique_ratio = valid.nunique() / len(valid) if len(valid) > 0 else 0

    # 自增 ID: 整数 + 唯一值 > 90% + 递增值
    if pd.api.types.is_integer_dtype(series) and unique_ratio > 0.90:
        if _is_progressive(valid):
            return "id", {"null_rate": null_rate, "reason": "递增值 (疑似自增主键)"}

    # 高唯一性整数 → 可能是 ID
    if pd.api.types.is_integer_dtype(series) and unique_ratio > 0.95:
        return "id", {"null_rate": null_rate, "reason": f"唯一值比例 {unique_ratio:.0%}"}

    # 哨兵值检测
    vals = set(valid.dropna().unique()[:20])
    sentinel_hits = vals & _SENTINEL_VALUES
    if sentinel_hits:
        logger.warning("列 [%s] 检测到哨兵值: %s", name, sentinel_hits)

    # 正常数值列
    return "numeric", {"null_rate": null_rate, "unique_ratio": unique_ratio}


def _is_progressive(series: pd.Series) -> bool:
    """检测是否为递增值 (1,2,3,4...) 或等差数列（保持原始顺序）"""
    s = series.dropna().head(100)
    if len(s) < 3:
        return False
    diffs = s.diff().dropna()
    if len(diffs) == 0:
        return False
    # 取 diffs 的众数作为步长，检查一致性
    mode_diff = diffs.mode().iloc[0] if not diffs.mode().empty else 0
    if mode_diff <= 0:
        return False
    same_step = (diffs == mode_diff).mean()
    return same_step > 0.80


# ============================================================
# Object 列二次探测
# ============================================================

def _classify_object(series: pd.Series, name: str, n: int, null_rate: float) -> tuple[str, dict]:
    """对 object 列进行：日期? 数值含符号? 百分比? 布尔? 高基数? 纯分类?"""
    valid = series.dropna()
    if len(valid) == 0:
        return "drop", {"reason": "无有效值"}

    # 1) 日期探测：先正则预检（避免对非日期列调用昂贵的 pd.to_datetime）
    _DATE_SNIFF = re.compile(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{4}")
    sample_vals = valid.drop_duplicates().head(10)
    first_few = [str(v) for v in sample_vals if pd.notna(v)]
    looks_like_date = any(_DATE_SNIFF.search(v) for v in first_few) or _DATE_NAME_PATTERN.search(name)
    if looks_like_date:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                converted = pd.to_datetime(sample_vals.head(30), errors="coerce")
            if len(sample_vals) > 0 and converted.notna().mean() > 0.80:
                return "date", {"null_rate": null_rate, "method": "infer"}
        except Exception:
            pass

    # 2) 布尔探测：仅两个唯一值 + 常见布尔词汇
    uniqs = valid.drop_duplicates().head(20)
    if len(uniqs) <= 4:
        lower_vals = {str(v).strip().lower() for v in uniqs}
        bool_sets = [
            {"yes", "no"}, {"true", "false"}, {"y", "n"},
            {"0", "1"}, {"t", "f"},
        ]
        for bs in bool_sets:
            if lower_vals.issubset(bs) or bs.issubset(lower_vals):
                return "bool", {"null_rate": null_rate, "method": "content"}

    # 3) 数值含符号探测：千分位、货币、百分比
    numeric_fixable, fix_note = _detect_numeric_string(valid)
    if numeric_fixable:
        return "numeric", {"null_rate": null_rate, "method": "string_parse", "note": fix_note}

    # 4) 高基数检测：>90% 唯一 → 可能是 ID 或自由文本
    unique_ratio = valid.nunique() / len(valid)
    if unique_ratio > 0.90:
        # 列名暗示是 ID
        if _ID_NAME_PATTERN.search(name):
            return "id", {"null_rate": null_rate, "reason": "列名ID模式+高唯一性"}
        # 超高基数但没名字暗示 → 可能是 UUID/自由文本，标记为 id 避免 GROUP BY 爆炸
        if unique_ratio > 0.98:
            return "id", {"null_rate": null_rate, "reason": f"超高基数 {unique_ratio:.1%}，排除出分类维度"}
        # 90-98% → 仍可能有用，标记为 categorical 加 warning
        logger.warning("列 [%s] 高基数 %.0f%%，GROUP BY 可能产生大量分组", name, unique_ratio * 100)

    # 5) 兜底：分类列
    return "categorical", {"null_rate": null_rate, "unique_count": valid.nunique()}


def _detect_numeric_string(series: pd.Series) -> tuple[bool, str]:
    """检测 object 列是否为含千分位/货币/百分比的数值字符串"""
    sample = series.dropna().head(100)
    if len(sample) == 0:
        return False, ""

    has_comma = sample.str.contains(r"\d,\d", regex=True).mean()
    has_currency = sample.str.contains(r"[$€£¥₹]", regex=True).mean()
    has_percent = sample.str.endswith("%").mean()
    has_scientific = sample.str.contains(r"\d+\.?\d*[eE][+-]?\d+", regex=True).mean()

    # 百分比
    if has_percent > 0.30:
        clean = sample.str.replace("%", "", regex=False).str.strip()
        try:
            pd.to_numeric(clean, errors="raise")
            return True, "百分比字符串"
        except ValueError:
            pass

    # 货币
    if has_currency > 0.30:
        clean = sample.str.replace(r"[$€£¥₹]", "", regex=True).str.replace(",", "", regex=False).str.strip()
        try:
            pd.to_numeric(clean, errors="raise")
            return True, "含货币符号"
        except ValueError:
            pass

    # 千分位
    if has_comma > 0.30:
        clean = sample.str.replace(",", "", regex=False).str.strip()
        try:
            pd.to_numeric(clean, errors="raise")
            return True, "千分位分隔符"
        except ValueError:
            pass

    # 科学计数
    if has_scientific > 0.10:
        try:
            pd.to_numeric(sample, errors="raise")
            return True, "科学计数法"
        except ValueError:
            pass

    return False, ""


# ============================================================
# 角色分配
# ============================================================

def _assign_role(schema: RuntimeSchema, col: str, role: str, detail: dict):
    """将分类结果填入 schema"""
    if role == "date":
        schema.date_columns.append(col)
    elif role == "numeric":
        schema.numeric_columns.append(col)
    elif role == "categorical":
        schema.categorical_columns.append(col)
    elif role == "id":
        schema.id_columns.append(col)
    elif role == "bool":
        schema.bool_columns.append(col)
    elif role == "drop":
        schema.drop_columns.append(col)

    # 自动填充质量检查列名
    if role in ("date", "numeric", "categorical"):
        if role == "date":
            schema.quality_critical_nulls.append(col)


def clean_numeric_string(series: pd.Series) -> pd.Series:
    """将检测到的含符号数值字符串清洗为 float"""
    s = series.astype(str).str.strip()
    s = s.str.replace("%", "", regex=False)
    s = s.str.replace(r"[$€£¥₹]", "", regex=True)
    s = s.str.replace(",", "", regex=False)
    return pd.to_numeric(s, errors="coerce")


def infer_and_clean_dates(df: pd.DataFrame, date_cols: list[str]) -> pd.DataFrame:
    """对检测到的日期列统一转为 datetime"""
    df = df.copy()
    for col in date_cols:
        if col not in df.columns:
            continue
        try:
            converted = pd.to_datetime(df[col], errors="coerce")
            ok = converted.notna().mean()
            if ok > 0.50:
                df[col] = converted
                logger.info("日期列 [%s]: 转换率 %.0f%%", col, ok * 100)
            else:
                logger.warning("日期列 [%s]: 转换率仅 %.0f%%，跳过", col, ok * 100)
        except Exception:
            pass
    return df
