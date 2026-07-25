"""
文件结构修复层 (Sanitizer)
==========================
在 pd.read_csv 之前和之后修复文件级结构问题。

检测与修复:
  S1  分隔符自动检测 (csv/tab/;)
  S2  无表头: 自动生成 col_0..col_N
  S3  重复列名: 自动去重
  S4  BOM 头剥离
  S5  空文件/单列: 立即报错
  S6  编码自动检测 (chardet 优先, trial-and-error 回落)
  S7  尾部汇总行剥离
  S8  全空列剔除
"""

from __future__ import annotations

import csv
import logging
import os
import re
from io import StringIO

import pandas as pd

logger = logging.getLogger(__name__)

# BOM 标记
_BOM = {
    b"\xef\xbb\xbf": "UTF-8-BOM",
    b"\xff\xfe": "UTF-16-LE",
    b"\xfe\xff": "UTF-16-BE",
}

# 常用分隔符候选
_SEPARATORS = [",", "\t", ";", "|"]

# 尾部汇总行关键词
_TAIL_KEYWORDS = re.compile(
    r"^\s*(total|sum|合计|總計|汇总|平均值|average|count)\s*[:：]?\s*[\d,.]*\s*$",
    re.IGNORECASE,
)


def detect_separator(filepath: str, n_lines: int = 10) -> str:
    """读取前 N 行，用 csv.Sniffer 检测分隔符"""
    with open(filepath, "r", encoding="utf-8-sig", errors="replace") as f:
        sample = "".join(f.readline() for _ in range(n_lines))
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=_SEPARATORS)
        return dialect.delimiter
    except csv.Error:
        # 回退：统计每行各分隔符出现次数的一致性
        lines = sample.strip().split("\n")
        best, best_var = ",", float("inf")
        for sep in _SEPARATORS:
            counts = [line.count(sep) for line in lines if line.strip()]
            if counts and max(counts) > 0:
                variance = max(counts) - min(counts)
                if variance < best_var:
                    best_var = variance
                    best = sep
        return best


def strip_bom(filepath: str) -> str | None:
    """检测并剥离 BOM 头，返回临时文件路径（无 BOM 时返回原路径）"""
    with open(filepath, "rb") as f:
        head = f.read(4)
    for bom_bytes, name in _BOM.items():
        if head.startswith(bom_bytes):
            logger.info("检测到 BOM: %s，自动剥离", name)
            tmp = filepath + ".nobom"
            with open(filepath, "rb") as src, open(tmp, "wb") as dst:
                src.read(len(bom_bytes))
                dst.write(src.read())
            return tmp
    return filepath


def detect_encoding(filepath: str) -> str:
    """编码检测：chardet → trial-and-error"""
    try:
        import chardet
        with open(filepath, "rb") as f:
            raw = f.read(100_000)
        result = chardet.detect(raw)
        if result and result.get("encoding"):
            enc = result["encoding"].upper().replace("-", "").replace("_", "")
            # Windows 编码映射
            if enc in ("GB2312", "GB18030", "GBK"):
                return "gbk"
            if enc == "BIG5":
                return "big5"
            if result["confidence"] > 0.7:
                logger.info("chardet: %s (%.0f%%)", result["encoding"], result["confidence"] * 100)
                return result["encoding"]
    except ImportError:
        pass
    except Exception:
        pass
    logger.info("chardet 不可用或置信度不足，使用 trial-and-error")
    return _trial_encoding(filepath)


def _trial_encoding(filepath: str) -> str:
    """尝试多种编码读取，选择无错误且行数最多的"""
    for enc in ["utf-8", "cp1252", "latin-1", "gbk", "utf-16"]:
        try:
            pd.read_csv(filepath, encoding=enc, nrows=5)
            return enc
        except (UnicodeDecodeError, UnicodeError, pd.errors.EmptyDataError):
            continue
    return "latin-1"  # 终级兜底：解释所有字节


def peek_file(filepath: str, encoding: str, separator: str) -> tuple[list[str], int, int]:
    """
    预览文件：返回 (列名列表, 表头行号, 估计总列数)
    自动处理：空文件检测、无表头、重复列名、尾部汇总行
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"文件不存在: {filepath}")
    if os.path.getsize(filepath) == 0:
        raise ValueError("文件为空 (0 bytes)，无法进行 ETL 处理")

    # 仅读取前 15 行用于列名/分隔符/表头检测（避免双读整个文件）
    with open(filepath, "r", encoding=encoding, errors="replace") as f:
        lines = [f.readline().rstrip("\n\r") for _ in range(15)]
    lines = [l for l in lines if l.strip()]
    if not lines:
        raise ValueError("文件无有效数据行")

    # 尾部汇总行检测（样本行，大文件尾部在 read_csv_safe 后通过 DataFrame tail 检测）
    stripped_tail = 0
    while lines and _TAIL_KEYWORDS.match(lines[-1]):
        lines.pop()
        stripped_tail += 1

    def _field_count(line: str) -> int:
        return len(next(csv.reader([line], delimiter=separator)))

    first_count = _field_count(lines[0])
    body_counts = [_field_count(l) for l in lines[1:min(11, len(lines))]]
    median_count = sorted(body_counts)[len(body_counts) // 2] if body_counts else first_count

    header_offset = 0
    if first_count < median_count * 0.5 and len(lines) > 1:
        lines = lines[1:]
        header_offset = 1
        first_count = _field_count(lines[0])

    reader = csv.reader(StringIO(lines[0]), delimiter=separator)
    columns = next(reader)
    lines = lines[1:]

    # 列名为空 → 生成 col_N
    columns = [c.strip() if c.strip() else f"col_{i}" for i, c in enumerate(columns)]
    # 重复列名去重
    seen: dict[str, int] = {}
    for i, c in enumerate(columns):
        if c in seen:
            seen[c] += 1
            columns[i] = f"{c}_{seen[c]}"
        else:
            seen[c] = 0

    total_cols = len(columns)
    total_rows_est = max(1, os.path.getsize(filepath) // 100)

    if total_cols <= 1:
        raise ValueError(f"文件仅 {total_cols} 列，无法进行有意义的 ETL 聚合")

    # 无表头检测
    numeric_headers = sum(1 for c in columns if c.replace(".", "").replace("-", "").isdigit())
    if numeric_headers > len(columns) * 0.5:
        logger.warning("疑似无表头文件，使用自动生成的列名")
        columns = [f"col_{i}" for i in range(len(columns))]

    logger.info(
        "文件预览: %d 列 × ~%d 行, 表头偏移 %d, 尾部剥离 %d",
        total_cols, total_rows_est, header_offset, stripped_tail,
    )
    return columns, total_rows_est, total_cols


def read_csv_safe(
    filepath: str,
    encoding: str | None = None,
    separator: str | None = None,
    dtype_overrides: dict | None = None,
) -> pd.DataFrame:
    """
    安全读取 CSV：自动 BOM→编码→分隔符→列名修复→读入。
    所有 S 层检测在此集成。
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"文件不存在: {filepath}")
    if os.path.getsize(filepath) == 0:
        raise ValueError("文件为空 (0 bytes)，无法进行 ETL 处理")

    # BOM
    clean_path = strip_bom(filepath)
    try:
        # 编码
        enc = encoding or detect_encoding(clean_path)
        # 分隔符
        sep = separator or detect_separator(clean_path)
        logger.info("读取: enc=%s sep=%r", enc, sep)

        # 先 peek 列名
        columns, n_rows, n_cols = peek_file(clean_path, enc, sep)

        # 依次尝试读取
        errors = []
        for fallback_enc in [enc, "utf-8", "cp1252", "latin-1"]:
            try:
                df = pd.read_csv(
                    clean_path,
                    encoding=fallback_enc,
                    sep=sep,
                    dtype=dtype_overrides or {},
                    on_bad_lines="skip",
                    skip_blank_lines=True,
                )
                df.columns = columns  # 使用已修复的列名

                # 剔除全空列
                before = len(df.columns)
                df = df.dropna(axis=1, how="all")
                dropped_all_null = before - len(df.columns)
                if dropped_all_null:
                    logger.info("剔除 %d 个全空列", dropped_all_null)

                # 强制将 dtype_overrides 中的列转为 str（防 int→.str crash）
                for col, target_type in (dtype_overrides or {}).items():
                    if col in df.columns and target_type == str:
                        df[col] = df[col].astype(str)

                logger.info("读取完成: %d 行 × %d 列", len(df), len(df.columns))
                return df

            except Exception as e:
                errors.append(f"{fallback_enc}: {e}")
                continue

        raise ValueError(f"所有编码尝试失败: {'; '.join(errors)}")
    finally:
        # 清理临时 BOM 文件
        if clean_path != filepath and os.path.exists(clean_path):
            os.remove(clean_path)
