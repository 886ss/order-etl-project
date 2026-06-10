"""
订单数据每日 ETL 调度 DAG
==========================
名称: daily_order_pipeline
调度: 每日凌晨 2:00 (Cron: 0 2 * * *)
任务: extract_orders → check_quality → build_dwd → build_dws → generate_report

特性：
- 任务依赖清晰
- 失败自动重试（3次，间隔5分钟）
- 执行日志自动记录到 etl_task_logs
"""

import sys
import os
from datetime import datetime, timedelta

# 将项目根目录加入 Python 路径
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from etl.extract import run_extract
from etl.quality import run_quality_check
from etl.transform import run_transform
from etl.aggregate import run_aggregate
from etl.report import run_report
from etl.logging_utils import log_task_start, log_task_success, log_task_failure

# ============================================================
# DAG 默认配置
# ============================================================
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# ============================================================
# 数据文件路径配置
# ============================================================
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "online_retail.csv")
REPORT_DIR = os.path.join(PROJECT_ROOT, "reports")

# ============================================================
# ETL 任务包装函数（带日志记录）
# ============================================================

def task_extract(**context):
    """Step 1: 数据抽取"""
    log_id = log_task_start("extract_orders")
    start = datetime.now()
    try:
        result = run_extract(DATA_PATH)
        if result["status"] == "success":
            log_task_success(log_id, start)
        else:
            log_task_failure(log_id, result.get("error", "unknown"), start)
            raise Exception(result.get("error", "extract failed"))
    except Exception as e:
        log_task_failure(log_id, str(e), start)
        raise


def task_quality(**context):
    """Step 2: 数据质量检查"""
    log_id = log_task_start("check_quality")
    start = datetime.now()
    try:
        result = run_quality_check()
        if result["status"] in ("passed", "warning"):
            log_task_success(log_id, start)
        else:
            log_task_failure(log_id, result.get("error", "unknown"), start)
            raise Exception(result.get("error", "quality check failed"))
    except Exception as e:
        log_task_failure(log_id, str(e), start)
        raise


def task_build_dwd(**context):
    """Step 3: 构建 DWD 层"""
    log_id = log_task_start("build_dwd")
    start = datetime.now()
    try:
        result = run_transform()
        if result["status"] == "success":
            log_task_success(log_id, start)
        else:
            log_task_failure(log_id, result.get("error", "unknown"), start)
            raise Exception(result.get("error", "build dwd failed"))
    except Exception as e:
        log_task_failure(log_id, str(e), start)
        raise


def task_build_dws(**context):
    """Step 4: 构建 DWS 层"""
    log_id = log_task_start("build_dws")
    start = datetime.now()
    try:
        result = run_aggregate()
        if result["status"] == "success":
            log_task_success(log_id, start)
        else:
            log_task_failure(log_id, result.get("error", "unknown"), start)
            raise Exception(result.get("error", "build dws failed"))
    except Exception as e:
        log_task_failure(log_id, str(e), start)
        raise


def task_generate_report(**context):
    """Step 5: 生成日报"""
    log_id = log_task_start("generate_report")
    start = datetime.now()
    try:
        result = run_report(REPORT_DIR)
        if result["status"] in ("success", "warning"):
            log_task_success(log_id, start)
        else:
            log_task_failure(log_id, result.get("error", "unknown"), start)
            raise Exception(result.get("error", "report generation failed"))
    except Exception as e:
        log_task_failure(log_id, str(e), start)
        raise


# ============================================================
# DAG 定义
# ============================================================
with DAG(
    dag_id="daily_order_pipeline",
    default_args=default_args,
    description="订单数据离线数仓每日 ETL 调度",
    schedule_interval="0 2 * * *",
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=["etl", "order", "warehouse"],
    doc_md=__doc__,
) as dag:

    # 任务节点
    extract_orders = PythonOperator(
        task_id="extract_orders",
        python_callable=task_extract,
        doc="从 CSV 文件抽取订单数据写入 ODS 层",
    )

    check_quality = PythonOperator(
        task_id="check_quality",
        python_callable=task_quality,
        doc="执行数据质量检查（空值/重复/异常值）",
    )

    build_dwd = PythonOperator(
        task_id="build_dwd",
        python_callable=task_build_dwd,
        doc="清洗 ODS 数据，构建 DWD 明细层",
    )

    build_dws = PythonOperator(
        task_id="build_dws",
        python_callable=task_build_dws,
        doc="按日聚合 DWD 数据，构建 DWS 汇总层",
    )

    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=task_generate_report,
        doc="从 DWS 生成每日运营日报",
    )

    finish = DummyOperator(
        task_id="finish",
        doc="ETL 流程结束标记",
    )

    # 任务依赖链
    extract_orders >> check_quality >> build_dwd >> build_dws >> generate_report >> finish
