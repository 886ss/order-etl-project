"""
订单数据每日 ETL 调度 DAG
==========================
名称: daily_order_pipeline
调度: 每日凌晨 2:00 (Cron: 0 2 * * *)
任务: extract_orders → check_quality → build_dwd → build_dws → generate_report

特性：
- 任务依赖清晰
- 失败自动重试（3次，间隔5分钟）
- 失败自动多通道告警（企业微信/飞书/邮件，按需配置）
- 执行日志自动记录到 etl_task_logs
- 支持全量/增量双模式（INCREMENTAL_MODE 开关）
  全量（默认）：每次 TRUNCATE + INSERT，适用于静态数据集演示
  增量：按 execution_date 抽取新数据并追加，适用于持续产出的生产数据源
"""

import sys
import os
from datetime import datetime, timedelta, timezone

# ============================================================
# 运行模式配置
# ============================================================
# False（全量）：每次 TRUNCATE + INSERT，适用于 UCI 静态数据集演示
# True （增量）：每次按 execution_date 抽取新数据追加，适用于生产数据源
# 注意：首次启动或更换数据集时，应先以全量模式运行一次初始化 ODS
INCREMENTAL_MODE = False

# 将项目根目录加入 Python 路径
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

try:
    from etl.extract import run_extract
    from etl.quality import run_quality_check
    from etl.transform import run_transform
    from etl.aggregate import run_aggregate
    from etl.report import run_report
    from etl.logging_utils import log_task_start, log_task_success, log_task_failure
    from etl.notify import send_alert
except ImportError as e:
    raise ImportError(
        f"{e}\n\n"
        "ETL 模块导入失败——DAG 文件必须保留在项目目录内运行。\n"
        "请使用以下方式之一部署：\n"
        "  方式A: export AIRFLOW_HOME=$(pwd) && airflow standalone\n"
        "  方式B: 设置 PYTHONPATH 指向项目根目录后启动 Airflow\n"
        "不要将 DAG 文件单独复制到 ~/airflow/dags/ 下。"
    )

# ============================================================
# DAG 默认配置
# ============================================================
def _on_task_failure(context: dict) -> None:
    """Airflow 失败回调（具名函数，支持序列化 DAG）。"""
    send_alert(
        f"ETL 失败: {context['task_instance'].task_id}",
        (
            f"DAG: {context['dag'].dag_id}\n"
            f"执行时间: {context.get('execution_date', 'N/A')}\n"
            f"任务: {context['task_instance'].task_id}\n"
            f"错误: {context.get('exception', 'unknown')}"
        ),
    )


default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _on_task_failure,
}

# ============================================================
# 数据文件路径配置
# ============================================================
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "online_retail.csv")
REPORT_DIR = os.path.join(PROJECT_ROOT, "reports")

# ============================================================
# ETL 任务包装函数（带日志记录）
# ============================================================

def _execute_task(task_name: str, run_func, *args) -> None:
    """
    通用 ETL 任务执行包装器，统一处理日志记录。

    run_* 函数内部已 try/except 返回 {"status": ...} 字典，
    不会向外抛异常。此处只检查 status 并记录一次日志。
    """
    log_id = log_task_start(task_name)
    start = datetime.now()
    try:
        result = run_func(*args)
    except Exception as e:
        # run_* 函数内部已全量 catch，此分支仅兜底防御
        log_task_failure(log_id, str(e), start)
        raise AirflowException(str(e)) from e

    if result["status"] in ("success", "passed", "warning", "skipped"):
        log_task_success(log_id, start)
    else:
        log_task_failure(log_id, result.get("error", "unknown"), start)
        raise AirflowException(result.get("error", f"{task_name} failed"))


def task_extract(**context):
    """Step 1: 数据抽取"""
    since_date = context.get("ds") if INCREMENTAL_MODE else None
    _execute_task("extract_orders", run_extract, DATA_PATH, INCREMENTAL_MODE, since_date)


def task_quality(**context):
    """Step 2: 数据质量检查"""
    _execute_task("check_quality", run_quality_check)


def task_build_dwd(**context):
    """Step 3: 构建 DWD 层"""
    since_date = context.get("ds") if INCREMENTAL_MODE else None
    _execute_task("build_dwd", run_transform, INCREMENTAL_MODE, since_date)


def task_build_dws(**context):
    """Step 4: 构建 DWS 层"""
    _execute_task("build_dws", run_aggregate)


def task_generate_report(**context):
    """Step 5: 生成日报"""
    _execute_task("generate_report", run_report, REPORT_DIR)


# ============================================================
# DAG 定义
# ============================================================
with DAG(
    dag_id="daily_order_pipeline",
    default_args=default_args,
    description="订单数据离线数仓每日 ETL 调度",
    schedule_interval="0 2 * * *",
    start_date=datetime(2020, 1, 1, tzinfo=timezone.utc),
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

    finish = EmptyOperator(
        task_id="finish",
        doc="ETL 流程结束标记",
    )

    # 任务依赖链
    extract_orders >> check_quality >> build_dwd >> build_dws >> generate_report >> finish
