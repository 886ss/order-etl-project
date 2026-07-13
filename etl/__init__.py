# Order Data Warehouse ETL Package

import logging

# 独立运行时激活日志输出；Airflow 中 basicConfig 是 no-op（root logger 已配置）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
