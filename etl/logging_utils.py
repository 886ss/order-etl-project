"""
ETL 任务日志工具
=================
提供任务执行日志记录功能，将每个 ETL 步骤的执行结果
写入 etl_task_logs 表，便于监控和排查。
"""

import logging
from datetime import datetime
from sqlalchemy import text
from etl.db import get_engine

logger = logging.getLogger(__name__)

_MAX_ERROR_LENGTH = 1000


def _safe_truncate(text: str, max_len: int = _MAX_ERROR_LENGTH) -> str:
    """安全截断字符串。Python 3 字符串切片按 code point 操作，不会切断多字节字符。"""
    return text[:max_len]


def log_task_start(task_name: str) -> int:
    """
    记录任务开始

    Args:
        task_name: 任务名称

    Returns:
        日志记录 ID
    """
    engine = get_engine()
    with engine.begin() as conn:
        result = conn.execute(
            text(
                "INSERT INTO etl_task_logs (task_name, status, start_time) "
                "VALUES (:name, 'running', :start_time) RETURNING id"
            ),
            {"name": task_name, "start_time": datetime.now()},
        )
        log_id = result.fetchone()[0]
    return log_id


def log_task_success(log_id: int, start_time: datetime) -> None:
    """
    记录任务成功完成

    Args:
        log_id: 日志记录 ID
        start_time: 任务开始时间
    """
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE etl_task_logs "
                "SET status = 'success', end_time = :end_time, "
                "duration = :duration "
                "WHERE id = :id"
            ),
            {"end_time": end_time, "duration": duration, "id": log_id},
        )


def log_task_failure(log_id: int, error_message: str, start_time: datetime) -> None:
    """
    记录任务执行失败

    Args:
        log_id: 日志记录 ID
        error_message: 错误信息
        start_time: 任务开始时间
    """
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE etl_task_logs "
                "SET status = 'failed', end_time = :end_time, "
                "duration = :duration, error_message = :error "
                "WHERE id = :id"
            ),
            {
                "end_time": end_time,
                "duration": duration,
                "error": _safe_truncate(error_message),
                "id": log_id,
            },
        )
