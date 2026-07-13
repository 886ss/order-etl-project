# Order Data Warehouse ETL

## 订单数据数仓 ETL 项目

![Python](https://img.shields.io/badge/Python-3.9+-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791)
![Airflow](https://img.shields.io/badge/Airflow-2.5+-017CEE)
![Tests](https://img.shields.io/badge/tests-34/34_passed-brightgreen)
![License](https://img.shields.io/badge/License-MIT-green)

---

## 📋 项目背景

基于 **Apache Airflow + PostgreSQL + Pandas** 构建的订单数据离线 ETL 流水线。采用经典 **ODS → DWD → DWS** 数仓分层架构，覆盖从数据抽取、质量检查、清洗转换、指标聚合到日报生成的完整链路。

**数据来源**：[UCI Online Retail Dataset](https://archive.ics.uci.edu/dataset/352/online+retail)（~54 万行英国电商交易记录）

---

## 🛠️ 技术栈

| 类别 | 技术 | 说明 |
|------|------|------|
| 语言 | Python 3.9+ | — |
| 数据库 | PostgreSQL | 业务表 + 任务日志表 |
| 调度 | Apache Airflow 2.5+ | DAG 编排、失败重试、日志监控 |
| 数据处理 | Pandas, NumPy | DataFrame 清洗聚合 |
| ORM / SQL | SQLAlchemy 2.0+ | Engine 单例 + 连接池 + 批量 UPSERT |
| 可视化 | Matplotlib | 架构图 / 数仓分层图 / DAG 流程图 |
| 日志 | Python logging | 统一替换 print，兼容 Airflow 日志系统 |
| 告警 | 策略模式多通道 | 企业微信 / 飞书 / SMTP 邮件，env 按需注册 |
| 测试 | pytest 9.x | 34 项单元测试，SQLite 内存库快速验证 |

---

## 🏗️ 项目架构

```
                  ┌──────────────────────────┐
                  │   Apache Airflow 调度层    │
                  │  daily_order_pipeline     │
                  │  每日 2:00 · 重试3次       │
                  └──────────┬───────────────┘
                             │
  ┌──────────────────────────┼──────────────────────────┐
  │                          │                          │
  ▼                          ▼                          ▼
┌─────────┐  extract   ┌─────────┐  aggregate   ┌─────────┐
│ ODS 层   │ ────────► │ DWD 层   │ ───────────► │ DWS 层   │
│ods_orders│ transform  │dwd_orders│              │dws_sales │
│ ~54万行  │            │ 清洗明细  │              │ _daily   │
└────┬────┘            └─────────┘              └────┬────┘
     │                                               │
     ▼                                               ▼
┌─────────┐                                     ┌─────────┐
│ CSV 文件 │                                     │ 日报输出 │
│Latin-1  │                                     │ .csv    │
└─────────┘                                     └─────────┘
```

---

## 📊 数仓分层设计

### ODS 层 — `ods_orders`

| 项目 | 说明 |
|------|------|
| 职责 | 原始订单数据镜像，不做业务处理 |
| 写入策略 | TRUNCATE + INSERT 原子事务，写入失败自动回滚 |
| 编码处理 | utf-8 → cp1252 → latin-1 自动回落，兼容多种 CSV 来源 |

### DWD 层 — `dwd_orders`

| 处理步骤 | 说明 |
|----------|------|
| 过滤取消订单 | `InvoiceNo` 以 'C' 开头标记为取消/退货 |
| 空值处理 | 剔除 `CustomerID` 为空的记录（无主订单） |
| 异常数量 | 剔除 `Quantity ≤ 0` 的行 |
| 异常单价 | 剔除 `UnitPrice ≤ 0` 的行 |
| 金额计算 | `order_amount = Quantity × UnitPrice`，显式 NUMERIC(12,4) |
| 时间戳 | 添加 `etl_time` 记录 ETL 处理时间 |

### DWS 层 — `dws_sales_daily`

| 指标 | 计算方式 | 说明 |
|------|---------|------|
| `daily_order_count` | `nunique(invoice_no)` | 日去重订单数 |
| `daily_customer_count` | `nunique(customer_id)` | 日去重客户数 |
| `daily_sales_amount` | `sum(order_amount)` | 日销售额 |
| `daily_avg_order_amount` | `sales / orders` | 客单价 |

写入策略：SQLAlchemy 批量 `INSERT ON CONFLICT DO UPDATE`，单条 SQL 完成全量 UPSERT。

---

## 🔄 Airflow DAG 流程

```
extract_orders       CSV → ODS（多编码自适应；全量 TRUNCATE+INSERT / 增量 APPEND 双模式）
     │
     ▼
check_quality        空值/重复/异常金额检查 + 非关键字段空值率阈值预警（默认30%，超限写warning不中断）
     │
     ▼
build_dwd            清洗 + 去重 + 金额计算 → DWD（全量/增量双模式）
     │
     ▼
build_dws            按日聚合 4 项指标 → DWS（批量 UPSERT，天生支持增量幂等）
     │
     ▼
generate_report      日报 CSV + 控制台输出
     │
     ▼
   finish            EmptyOperator 流程结束
```

| 特性 | 配置 |
|------|------|
| 调度频率 | 每日凌晨 2:00 (`0 2 * * *`) |
| 时区 | UTC（`datetime(2020, 1, 1, tzinfo=timezone.utc)`） |
| 失败重试 | 3 次，间隔 5 分钟 |
| 失败告警 | `on_failure_callback` 自动多通道告警（企业微信/飞书/邮件，按需配置） |
| 执行日志 | 自动写入 `etl_task_logs` 表，单次记录无重复 |
| 全量/增量 | `INCREMENTAL_MODE` 开关：全量（TRUNCATE+INSERT）或增量（按 execution_date 追加） |
| 日志方式 | Python `logging` 模块，兼容 Airflow 日志级别过滤 |

---

## 🗄️ 数据库表设计

| 表名 | 层级 | 行数（预估） | 说明 |
|------|------|-------------|------|
| `ods_orders` | ODS | ~540,000 | 原始订单数据镜像 |
| `dwd_orders` | DWD | ~400,000 | 清洗后订单明细，含 `order_amount` |
| `dws_sales_daily` | DWS | ~400 | 按日聚合的 4 项销售指标 |
| `etl_task_logs` | 日志 | 每任务 1 行 | ETL 步骤执行状态与耗时 |

### 查询索引

| 索引名 | 表 | 列 | 覆盖查询场景 |
|--------|---|-----|-------------|
| `idx_ods_invoice_date` | `ods_orders` | `invoice_date` | 按日期过滤 |
| `idx_dwd_invoice_date` | `dwd_orders` | `invoice_date` | 按日期聚合 |
| `idx_dwd_customer_id` | `dwd_orders` | `customer_id` | 按客户维度分析 |
| `idx_dwd_invoice_no` | `dwd_orders` | `invoice_no` | 订单去重/关联查询 |

### 约束保障

- `dwd_orders.customer_id` 设为 `NOT NULL`，与清洗逻辑 `dropna(subset=["customer_id"])` 形成数据库层面的防御
- `dws_sales_daily.stat_date` 设 `UNIQUE`，配合 `ON CONFLICT` UPSERT

---

## 🚀 快速开始

### 1. 环境准备

```bash
git clone https://github.com/886ss/order-etl-project.git
cd order-etl-project

# 安装依赖（开发环境建议用锁定版本）
pip install -r requirements.txt
# 或: pip install -r requirements-dev.txt

# 创建 PostgreSQL 数据库
createdb order_warehouse

# 配置环境变量（可选，默认连接 localhost:5432）
cp .env.example .env
# 编辑 .env: PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
```

### 2. 初始化表结构

```bash
psql -d order_warehouse -f sql/create_tables.sql

# 验证表是否正确创建
psql -d order_warehouse -f sql/verify_tables.sql
```

### 3. 下载数据集

```bash
python etl/download_data.py
# 自动下载 UCI Online Retail Dataset → data/online_retail.csv
```

### 4. 独立模式运行 ETL（无需 Airflow）

```bash
python etl/extract.py     # Step 1: CSV → ODS
python etl/quality.py     # Step 2: 数据质量检查
python etl/transform.py   # Step 3: ODS → DWD
python etl/aggregate.py   # Step 4: DWD → DWS
python etl/report.py      # Step 5: 生成日报
```

### 5. Airflow 调度模式

```bash
# 方式A: 项目根目录即 Airflow HOME（推荐）
cd order-etl-project
export AIRFLOW_HOME=$(pwd)
airflow standalone

# 方式B: 已有 Airflow 实例，设置 DAG 搜索路径 + PYTHONPATH
export AIRFLOW__CORE__DAGS_FOLDER=/path/to/order-etl-project/dags
export PYTHONPATH=/path/to/order-etl-project:$PYTHONPATH
# ⚠️ 不要将 DAG 文件单独复制到 ~/airflow/dags/，否则 import 路径会失效

# 访问 http://localhost:8080 查看 DAG 运行状态
```

### 6. 运行测试

```bash
pytest tests/ -v
# 34 passed — 覆盖 extract / quality / transform / aggregate / report / db / notify
```

---

## 📁 项目结构

```
order-etl-project/
├── etl/                            # ETL 核心模块
│   ├── db.py                       # 引擎单例 + 连接池 + 共享 ODS_DTYPE/DWD_DTYPE + truncate_and_load()
│   ├── extract.py                  # Step1: CSV → ODS（多编码自适应 + 原子事务）
│   ├── quality.py                  # Step2: 空值/重复/异常值检查 + 非关键字段空值率阈值预警（默认30%）
│   ├── transform.py                # Step3: ODS → DWD（清洗 + 金额计算 + 原子事务）
│   ├── aggregate.py                # Step4: DWD → DWS（批量 UPSERT + 缓存的表定义）
│   ├── report.py                   # Step5: 日报 CSV 生成
│   ├── logging_utils.py            # 任务执行日志表记录（安全字符串截断）
│   ├── notify.py                   # 多通道告警（企业微信/飞书/SMTP 邮件，策略模式 + env 按需注册）
│   └── download_data.py            # UCI 数据集下载（urlopen + csv/xlsx 双支持）
├── dags/
│   └── daily_order_pipeline.py     # Airflow DAG（通用 _execute_task 包装器，tz-aware）
├── sql/
│   ├── create_tables.sql           # 建表 + 索引 + COMMENT
│   └── verify_tables.sql           # 验证表结构（不插入数据）
├── tests/
│   ├── test_db.py                  # truncate_and_load 原子事务（SQLite）
│   ├── test_extract.py             # CSV 读取 + 列校验
│   ├── test_quality.py             # 质量检查 SQL + 空值率阈值预警（SQLite 内存库）
│   ├── test_transform.py           # 清洗逻辑 7 项测试
│   ├── test_aggregate.py           # 聚合计算 3 项测试
│   ├── test_report.py              # 日报摘要 + 文件保存
│   └── test_notify.py              # 多通道告警注册/广播/容错
├── docs/
│   ├── architecture.png            # 项目架构图
│   ├── warehouse_design.png        # 数仓分层图
│   ├── dag_flow.png                # DAG 流程图
│   ├── generate_diagrams.py        # 架构图生成脚本（跨平台中文字体自动检测）
├── data/                           # 数据集目录
│   └── online_retail.csv           # （需下载）
├── reports/                        # 日报输出目录
│   └── daily_report_YYYYMMDD.csv
├── requirements.txt                # 宽松约束（>=）
├── requirements-dev.txt            # 精确锁定版本
├── .env.example                    # 环境变量模板
├── .gitignore                      # 排除 .env / __pycache__ / Airflow 运行时
└── README.md
```

---

## ✨ 项目亮点

### 架构设计

1. **数仓分层**：ODS → DWD → DWS 三层解耦，每层职责单一、可独立测试
2. **全量/增量双模式**：`INCREMENTAL_MODE` 开关一键切换；全量 TRUNCATE+INSERT 原子事务；增量按日期追加 + DWS UPSERT 天然幂等
3. **单例连接池**：Engine 模块级单例 + pool_size/max_overflow/pool_recycle 配置，杜绝连接泄漏

### 数据处理

1. **多编码自适应**：CSV 读取自动 utf-8 → cp1252 → latin-1 回落，适配真实数据集
2. **批量 UPSERT**：SQLAlchemy `pg_insert.on_conflict_do_update()` 替代逐行 iterrows，单条 SQL 完成
3. **显式类型映射**：to_sql 使用共享 `ODS_DTYPE` / `DWD_DTYPE` 常量，防止 NUMERIC→FLOAT 精度丢失

### 质量与测试

1. **数据质量检查**：4 维度 SQL 检查（空值/重复/异常金额/空值率），`CASE WHEN` 语法兼容 PostgreSQL + SQLite
2. **空值率阈值预警**：非关键字段空值率超过阈值（默认 30%）写入 warning 日志，不中断管线，兼顾日报产出与数据质量追溯
3. **34 项单元测试**：extract(4) + quality(7) + transform(7) + aggregate(3) + report(4) + db(4) + notify(5)，SQLite 内存库秒级验证
4. **数据库层防御**：DWD `customer_id NOT NULL` 约束 + 4 个查询索引 + 上游空表自动检测（skipped 状态，不静默 pass）

### 工程实践

1. **多通道告警**：策略模式可插拔告警（企业微信/飞书/SMTP），env 驱动按需注册，单通道异常不影响其他通道
2. **日志体系**：Python `logging` 模块替代 print，兼容 Airflow 日志级别过滤；`_safe_truncate()` 安全截断
3. **DAG 代码精简**：通用 `_execute_task()` 包装器消除 5 个重复的任务函数；`on_failure_callback` 自动告警；`EmptyOperator` + tz-aware start_date
4. **跨平台兼容**：架构图生成脚本自动检测 Windows/macOS/Linux 中文环境字体
5. **环境可复现**：`requirements-dev.txt` 精确锁版本；`.env.example` 模板（含告警通道）；`.gitignore` 排除敏感文件

---

## 📄 License

MIT
