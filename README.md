# Order Data Warehouse ETL Project
## 订单数据数仓ETL项目

![Python](https://img.shields.io/badge/Python-3.9+-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791)
![Airflow](https://img.shields.io/badge/Airflow-2.5+-017CEE)
![License](https://img.shields.io/badge/License-MIT-green)

---

## 📋 项目背景

本项目模拟电商平台订单分析场景，基于 **Airflow** 构建离线 ETL 调度流程，实现订单数据从抽取、清洗、转换到指标统计的全自动化处理。

采用经典的 **ODS → DWD → DWS** 数仓分层架构，基于 **PostgreSQL** 存储业务数据及任务日志，支持任务依赖管理、失败重试与执行监控。

> 🎯 定位：本科数据开发/大数据开发/数仓开发实习生求职展示项目
>
> 📖 **面试准备**：👉 [面试准备指南 (INTERVIEW_GUIDE.md)](docs/INTERVIEW_GUIDE.md) — 含 20 个高频面试问题 + STAR 话术

---

## 🛠️ 技术栈

| 类别 | 技术 |
|------|------|
| 语言 | Python 3.9+ |
| 数据库 | PostgreSQL |
| 调度 | Apache Airflow 2.5+ |
| 数据处理 | Pandas, NumPy |
| ORM | SQLAlchemy |
| 可视化 | Matplotlib |

---

## 🏗️ 项目架构

```
                    ┌──────────────────────────┐
                    │    Apache Airflow 调度     │
                    │  daily_order_pipeline     │
                    └──────────┬───────────────┘
                               │
    ┌──────────────────────────┼──────────────────────────┐
    │                          │                          │
    ▼                          ▼                          ▼
┌─────────┐   extract   ┌─────────┐   aggregate   ┌─────────┐
│  ODS 层  │ ──────────► │  DWD 层  │ ────────────► │  DWS 层  │
│ ods_orders│  transform  │dwd_orders│               │dws_sales │
└─────────┘             └─────────┘               │ _daily   │
     ▲                                              └────┬────┘
     │                                                   │
     │                                                   ▼
┌─────────┐                                       ┌─────────┐
│   CSV    │                                       │  日报输出 │
│ 订单数据  │                                       │  .csv    │
└─────────┘                                       └─────────┘
```

---

## 📊 数仓分层设计

### ODS 层 — `ods_orders`
| 职责 | 保存原始订单数据镜像 |
|------|---------------------|
| 原则 | 不做任何业务处理，与源数据保持一致 |

### DWD 层 — `dwd_orders`
| 处理 | 说明 |
|------|------|
| 空值处理 | 剔除 CustomerID 为空的记录 |
| 异常值 | 剔除 Quantity ≤ 0、UnitPrice ≤ 0 |
| 订单去重 | 过滤取消订单（InvoiceNo 以 'C' 开头） |
| 金额计算 | `order_amount = Quantity × UnitPrice` |
| 时间戳 | 添加 `etl_time` 记录处理时间 |

### DWS 层 — `dws_sales_daily`
| 指标 | 说明 |
|------|------|
| `daily_order_count` | 日订单数（去重） |
| `daily_customer_count` | 日客户数（去重） |
| `daily_sales_amount` | 日销售额 |
| `daily_avg_order_amount` | 客单价（销售额／订单数） |

---

## 🔄 Airflow DAG 流程

```
extract_orders       从 CSV 抽取 → ODS
     │
     ▼
check_quality         空值/重复/异常值检查
     │
     ▼
build_dwd             清洗 + 去重 + 金额计算 → DWD
     │
     ▼
build_dws             按日聚合指标 → DWS
     │
     ▼
generate_report        生成日报 CSV + 控制台输出
     │
     ▼
   finish              流程结束
```

**调度特性：**
- 每日凌晨 2:00 自动执行（`0 2 * * *`）
- 任务失败自动重试 3 次（间隔 5 分钟）
- 每步自动记录执行日志到 `etl_task_logs`

---

## 🗄️ 数据库表设计

| 表名 | 层级 | 说明 |
|------|------|------|
| `ods_orders` | ODS | 原始订单数据镜像 |
| `dwd_orders` | DWD | 清洗后订单明细 |
| `dws_sales_daily` | DWS | 销售主题日度汇总 |
| `etl_task_logs` | 日志 | ETL 任务执行日志 |

---

## 🚀 运行步骤

### 1. 环境准备

```bash
# 克隆项目
git clone <your-repo-url>
cd order-etl-project

# 安装依赖
pip install -r requirements.txt

# 安装 PostgreSQL 并创建数据库
createdb order_warehouse

# 配置数据库连接（可选，默认连接本地 PostgreSQL）
cp .env.example .env
# 编辑 .env 设置 PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
```

### 2. 初始化表结构

```bash
psql -d order_warehouse -f sql/create_tables.sql
```

### 3. 下载数据集

```bash
python etl/download_data.py
```

### 4. 运行 ETL（独立模式）

```bash
# 逐步骤运行
python etl/extract.py data/online_retail.csv
python etl/quality.py
python etl/transform.py
python etl/aggregate.py
python etl/report.py
```

### 5. 启动 Airflow 调度

```bash
# 启动 Airflow Standalone
airflow standalone

# 或复制 DAG 到 Airflow dags 目录
cp dags/daily_order_pipeline.py $AIRFLOW_HOME/dags/

# 访问 http://localhost:8080 查看 DAG 运行状态
```

### 6. 运行测试

```bash
pytest tests/ -v
```

---

## 📁 项目目录结构

```
order-etl-project/
├── data/
│   └── online_retail.csv          # 原始数据集（需下载）
├── etl/
│   ├── __init__.py
│   ├── db.py                      # 数据库连接管理
│   ├── extract.py                 # Step1: CSV → ODS
│   ├── quality.py                 # Step2: 数据质量检查
│   ├── transform.py               # Step3: ODS → DWD
│   ├── aggregate.py               # Step4: DWD → DWS
│   ├── report.py                  # Step5: 日报生成
│   ├── logging_utils.py           # 任务日志工具
│   └── download_data.py           # 数据集下载
├── dags/
│   └── daily_order_pipeline.py    # Airflow DAG
├── sql/
│   ├── create_tables.sql          # 建表脚本
│   └── init_data.sql              # 初始化脚本
├── tests/
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_quality.py
│   ├── test_transform.py
│   ├── test_aggregate.py
│   └── test_report.py
├── reports/                       # 日报输出目录
├── docs/                          # 文档/架构图
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## ✨ 项目亮点

1. **数仓分层思想**：ODS → DWD → DWS 三层架构，职责清晰，易于扩展
2. **完整 ETL 流程**：抽取 → 清洗 → 转换 → 聚合 → 报表，每个环节可独立运行
3. **Airflow 任务调度**：任务依赖管理、失败自动重试、执行日志监控
4. **数据质量管理**：空值检测、重复检查、异常金额预警
5. **PostgreSQL 实战**：CTAS/TRUNCATE/UPSERT 多种写入策略，日志表自动记录
6. **21 项单元测试**：全模块 pytest 覆盖，支持 SQLite 内存库快速验证

---

## 📝 简历描述（可直接使用）

> **订单数据数仓ETL项目**
>
> 技术栈：Python｜PostgreSQL｜Airflow｜Pandas
>
> - 基于 Airflow 构建离线 ETL 调度流程，实现订单数据抽取、清洗、转换及指标统计自动化运行；
> - 设计 ODS→DWD→DWS 数仓分层结构，完成订单明细加工与销售主题指标汇总；
> - 基于 PostgreSQL 存储业务数据及任务日志，实现任务依赖管理、失败重试与执行监控；
> - 构建日报自动生成功能，输出销售额、订单数、用户数及客单价等核心运营指标。

---

## 📄 License

MIT
