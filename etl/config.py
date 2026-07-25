"""
ETL 配置管理器
==============
YAML schema 加载 + 运行时推断结果缓存。

两模式：
  自动模式（无 YAML）：profiler 推断 + 缓存到 schema_cache.json
  覆盖模式（有 YAML）：schema.yaml 优先，缺失字段由 profiler 补全
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# 默认缓存路径
CACHE_FILE = "data/schema_cache.json"


# ============================================================
# 运行时 Schema 数据类
# ============================================================

@dataclass
class RuntimeSchema:
    """profiler 推断/ YAML 加载后的统一内部表示"""
    source_mapping: dict[str, str] = field(default_factory=dict)
    dtypes: dict[str, str] = field(default_factory=dict)
    date_columns: list[str] = field(default_factory=list)
    numeric_columns: list[str] = field(default_factory=list)
    categorical_columns: list[str] = field(default_factory=list)
    id_columns: list[str] = field(default_factory=list)
    bool_columns: list[str] = field(default_factory=list)
    drop_columns: list[str] = field(default_factory=list)
    csv_encodings: list[str] = field(
        default_factory=lambda: ["utf-8", "cp1252", "latin-1"]
    )
    csv_dtype_overrides: dict[str, str] = field(default_factory=dict)
    cancel_rules: list[dict] = field(default_factory=list)
    required_columns: list[dict] = field(default_factory=list)
    validations: list[dict] = field(default_factory=list)
    computed_columns: list[dict] = field(default_factory=list)
    quality_critical_nulls: list[str] = field(default_factory=list)
    quality_null_rate_cols: list[str] = field(default_factory=list)
    quality_null_threshold: float = 0.30


# ============================================================
# 公共 API
# ============================================================

def load_yaml_schema(path: str | None = None) -> dict | None:
    """加载 YAML schema 文件，不存在时返回 None"""
    candidates = [path] if path else ["etl/schema.yaml", "schema.yaml"]
    for p in candidates:
        if p and os.path.exists(p):
            with open(p, encoding="utf-8") as f:
                logger.info("加载 Schema 配置: %s", p)
                return yaml.safe_load(f)
    return None


def yaml_to_runtime_schema(raw: dict) -> RuntimeSchema:
    """将 YAML dict 转换为 RuntimeSchema 数据类"""
    s = RuntimeSchema()
    if not raw:
        return s

    s.source_mapping = raw.get("source_column_mapping", {})
    s.csv_encodings = raw.get("csv_read", {}).get("encodings", s.csv_encodings)
    s.csv_dtype_overrides = raw.get("csv_read", {}).get("dtype_overrides", {})

    dt = raw.get("column_dtypes", {})
    for col, info in dt.items():
        t = info.get("type", "String")
        length = info.get("length", "")
        s.dtypes[col] = f"{t}({length})" if length else t

    cleaning = raw.get("cleaning", {})
    s.cancel_rules = cleaning.get("cancel_rules", [])
    s.required_columns = cleaning.get("required_columns", [])
    s.validations = cleaning.get("validations", [])
    s.computed_columns = cleaning.get("computed_columns", [])

    quality = raw.get("quality", {})
    s.quality_critical_nulls = quality.get("critical_null_columns", [])
    s.quality_null_rate_cols = quality.get("null_rate_columns", [])
    s.quality_null_threshold = quality.get("null_rate_threshold", 0.30)
    return s


def load_cache(filepath: str = CACHE_FILE) -> dict | None:
    """加载缓存的推断结果"""
    if os.path.exists(filepath):
        with open(filepath, encoding="utf-8") as f:
            return json.load(f)
    return None


def save_cache(schema: RuntimeSchema, filepath: str = CACHE_FILE):
    """将 RuntimeSchema 序列化到 JSON 缓存"""
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    d = {
        k: v for k, v in schema.__dict__.items()
        if not k.startswith("_")
    }
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(d, f, indent=2, ensure_ascii=False, default=str)


def merge_schemas(yaml_schema: RuntimeSchema, inferred: RuntimeSchema) -> RuntimeSchema:
    """YAML schema 优先，缺失字段由 inferred 补全。避免 deepcopy，选择性复制。"""
    # YAML 空 → 直接用推断结果
    if not yaml_schema.source_mapping and not yaml_schema.dtypes and not yaml_schema.cancel_rules:
        return inferred

    # 核心角色列表始终来自推断（YAML 不覆盖列角色）
    result = RuntimeSchema(
        date_columns=list(inferred.date_columns),
        numeric_columns=list(inferred.numeric_columns),
        categorical_columns=list(inferred.categorical_columns),
        id_columns=list(inferred.id_columns),
        bool_columns=list(inferred.bool_columns),
        drop_columns=list(inferred.drop_columns),
        source_mapping=yaml_schema.source_mapping or inferred.source_mapping,
        dtypes=yaml_schema.dtypes or inferred.dtypes,
    )
    # 规则字段：YAML 有就用 YAML，没有就用 inferred
    for field in ("cancel_rules", "required_columns", "validations", "computed_columns",
                  "quality_critical_nulls", "quality_null_rate_cols"):
        yaml_val = getattr(yaml_schema, field)
        setattr(result, field, yaml_val if yaml_val else getattr(inferred, field))
    result.quality_null_threshold = yaml_schema.quality_null_threshold
    result.csv_encodings = yaml_schema.csv_encodings
    result.csv_dtype_overrides = yaml_schema.csv_dtype_overrides
    return result
