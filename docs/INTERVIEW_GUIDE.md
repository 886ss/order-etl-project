# 面试准备指南：订单数据数仓ETL项目

> **目标岗位**：数据开发实习 / 数仓开发实习 / 大数据开发实习
> **读完本文时间**：30 分钟
> **面试前复习**：15 分钟看标注 `⭐` 段落

---

## 一、30秒电梯演讲

> 我基于 Airflow + PostgreSQL 构建了一个电商订单数据仓库 ETL 项目。核心是 **ODS → DWD → DWS 三层数仓架构**，从 CSV 原始数据出发，经过质量检查、清洗去重、金额计算，最终产出日度销售汇总报表。整个过程由 Airflow 每日自动调度运行，包含失败重试和日志监控。Python 代码约 900 行，21 项 pytest 全绿。

---

## 二、项目一句话定位 ⭐

**解决什么问题？**
> 电商每天产生大量订单 CSV，需要自动化地从"原始数据"变成"可看的日报"。

**怎么做？**
> 用 Airflow 每天凌晨自动跑 ETL：抽取 → 清洗 → 汇总 → 出报表。

**为什么这么设计？**
> 数仓分层是行业标准，分层后每层职责单一、问题好排查、方便复用。

---

## 三、数仓分层详解 ⭐⭐⭐ (面试必问)

### ODS 层 (Operational Data Store)

```
表名：ods_orders
职责：原始数据镜像，和 CSV 一模一样
原则：不动数据，不删不改
SQL策略：TRUNCATE + INSERT 全量刷新
```

**面试时你要说的**：
> "ODS 层就是数据湖的落地层。我保持了 CSV 的原始面貌，不做任何清洗。好处是：如果下游出了问题，我们可以回到 ODS 重新处理，不会丢失原始信息。"

### DWD 层 (Data Warehouse Detail)

```
表名：dwd_orders
职责：清洗后的订单明细
处理了什么：
  1. 过滤取消订单 —— InvoiceNo 以 'C' 开头的是退款单，不能算入销售
  2. 剔除空客户 —— CustomerID 为空无法做客户维度分析
  3. 剔除非正数 —— Quantity ≤ 0 或 UnitPrice ≤ 0 的业务含义是退货/赠品/错误数据
  4. 计算金额 —— order_amount = Quantity × UnitPrice
  5. 记录时间 —— etl_time 标记何时处理
新增字段：order_amount, etl_time
SQL策略：TRUNCATE + INSERT 全量刷新
```

**面试时你要说的** ⭐：
> "DWD 层的核心工作是**数据标准化**。比如 InvoiceNo 以 C 开头代表取消订单（Cancel），这是这个数据集的业务规则。Quantity 为负表示退货，UnitPrice 为 0 可能是赠品或录入错误。我把这些脏数据过滤掉，保证下游拿到的都是有效销售记录。"

### DWS 层 (Data Warehouse Summary)

```
表名：dws_sales_daily
职责：按日汇总
四个指标：
  - daily_order_count       → COUNT(DISTINCT invoice_no)
  - daily_customer_count    → COUNT(DISTINCT customer_id)
  - daily_sales_amount      → SUM(order_amount)
  - daily_avg_order_amount  → 销售额 ÷ 订单数 (客单价)
SQL策略：UPSERT（ON CONFLICT UPDATE，新日期插入，已有日期更新）
```

**面试时你要说的** ⭐：
> "DWS 层是面向主题的汇总层。我用 UPSERT 而不是 TRUNCATE 的原因是：如果后续有增量数据到达，同一天的指标可以直接更新而不会丢失其他日期的数据。客单价 = 销售额 / 订单数，这是电商最基础的效率指标。"

---

## 四、ETL 流程逐步骤 ⭐⭐

```
extract_orders        ───  读取 CSV → 列名校验 → 写入 ods_orders
        │
        ▼
check_quality         ───  空值/重复/异常金额 三大检查
        │               ───  生成质量报告（不阻断流程，只告警）
        ▼
build_dwd             ───  ODS 全量读取 → 5步清洗流水线 → 写入 dwd_orders
        │
        ▼
build_dws             ───  DWD 按日期聚合 → 4项指标 → UPSERT 写入 dws_sales_daily
        │
        ▼
generate_report       ───  DWS 读取 → 汇总统计 → 日报 CSV + 控制台输出
        │
        ▼
finish
```

**面试时你要说的**：
> "五个步骤之间有严格的依赖关系，Airflow 保证了上游失败下游不会执行。每一步都内置了异常捕获和日志记录，失败会自动重试 3 次，每次间隔 5 分钟。所有执行记录写入 `etl_task_logs` 表，可以通过 SQL 直接查历史任务的运行状态。"

---

## 五、技术选型理由 ⭐⭐

| 技术 | 为什么用它 | 可能问你的 |
|------|-----------|-----------|
| **Airflow** | 业界标准的调度框架，Python 原生支持，DAG 可视化 | "为什么不用 Cron？" — Cron 无法处理任务依赖和失败重试 |
| **PostgreSQL** | 开源关系型数据库，支持 UPSERT/窗口函数/CTE | "为什么不用 MySQL？" — PG 对分析型 SQL 支持更好 |
| **Pandas** | 数据清洗的瑞士军刀，DataFrame 操作直观 | "为什么不用纯 SQL？" — Python 里做复杂清洗比 SQL 更灵活 |
| **SQLAlchemy** | 避免拼接 SQL 字符串，防止注入 | "为什么不用 psycopg2 直连？" — ORM 层让代码更易维护 |
| **pytest** | Python 标准测试框架 | — |

---

## 六、代码精华速览

### 1. 清洗流水线（etl/transform.py:clean_data）

```python
# 面试亮点：链式清洗，每一步都有日志输出
df = df[~df["invoice_no"].str.startswith("C", na=False)]  # 取消订单
df = df.dropna(subset=["customer_id"])                      # 空客户
df = df[df["quantity"] > 0]                                  # 非正数量
df = df[df["unit_price"] > 0]                                # 非正单价
df["order_amount"] = df["quantity"] * df["unit_price"]       # 金额计算
df["etl_time"] = datetime.now()                              # 时间戳
```

### 2. UPSERT 策略（etl/aggregate.py:load_to_dws）

```python
# 面试亮点：ON CONFLICT 是 PG 特性，MySQL 不支持
INSERT INTO dws_sales_daily (...) VALUES (...)
ON CONFLICT (stat_date)
DO UPDATE SET
    daily_order_count = EXCLUDED.daily_order_count,
    ...
```

### 3. Airflow DAG 结构（dags/daily_order_pipeline.py）

```python
# 面试亮点：清晰的依赖声明
extract_orders >> check_quality >> build_dwd >> build_dws >> generate_report >> finish
```

---

## 七、20 个高频面试问题 ⭐⭐⭐

### 数仓基础

**Q1: 你的数仓分了几层？每层干什么？**
> 三层。ODS 存原始镜像不处理，DWD 做清洗和标准化，DWS 做按日汇总。这样分层后，如果日报数据有问题，我可以追溯到 DWD 看明细，再追溯到 ODS 看原始数据，一层层排查。

**Q2: 为什么 ODS 和 DWD 要分开？直接洗完后存一版不行吗？**
> 不行。分开是为了**数据可追溯**。如果清洗逻辑有 bug（比如不小心过滤了正常数据），重新从 ODS 跑一遍就能恢复。合在一起就丢失了原始信息。

**Q3: 增量同步和全量同步的区别？你怎么选的？**
> 全量是每次 TRUNCATE 再 INSERT，简单可靠，适合数据量不大的场景。增量是只处理新增/变更部分，适合大数据量。我这个项目日增量约几千行，用全量就行。DWS 层我用了 UPSERT，是增量思维——因为汇总表可能需要历史数据持续更新。

**Q4: 什么是缓慢变化维（SCD）？你的项目用到了吗？**
> SCD 是维度表处理历史变化的方法。我的项目目前维度简单（国家/商品代码基本不变），没专门处理。如果需要，可以用 SCD Type 2（加生效时间/失效时间字段保留历史快照）。

### ETL 开发

**Q5: 你的 ETL 流程中数据质量检查做了哪些？**
> 三项：空值检查（关键字段不能为空）、重复检查（同一 InvoiceNo 是否重复）、异常值检查（负单价/负数量/零值）。质量检查不阻断流程，只输出报告——因为数据质量问题不一定是需要停机的。

**Q6: Quantity 为负代表什么？怎么处理？**
> 负 Quantity 代表退货/退款。我的 DWD 层直接过滤掉了，因为分析只关注正向销售。如果要分析退货率，应该单独建一张退货事实表。

**Q7: 为什么用 Pandas 做清洗而不是纯 SQL？**
> Pandas 在 Python 里处理数据更灵活——比如字符串判断（`str.startswith('C')`）、条件过滤链式调用。SQL 当然也能做，但嵌在 Python 调度流程里，Pandas 更自然。小数据量下性能差异可忽略。

**Q8: order_amount 为什么在 DWD 层计算而不是在报表层？**
> 因为 DWD 层是明细层，Quantity × UnitPrice 是原子计算。如果放到报表层，每次出报告都要重新算，而且各报表可能算法不一致。在 DWD 层固化下来，下游所有分析都用同一个口径。

### Airflow 调度

**Q9: 什么是 DAG？你的 DAG 有向无环图怎么设计的？**
> DAG = Directed Acyclic Graph，有向无环图。我的 DAG 是纯线性的：extract → quality → dwd → dws → report → finish。每一步是一个 Task，箭头表示依赖。Airflow 保证按依赖顺序执行。

**Q10: 如果 build_dwd 失败了，会发生什么？**
> Airflow 会自动重试（我配置了 retries=3, retry_delay=5min）。重试 3 次仍失败，则该 Task 标记为 failed，下游 build_dws 和 generate_report 不会执行。同时失败信息会写入 etl_task_logs。

**Q11: backfill 是什么？你的 DAG 支持吗？**
> Backfill 是补跑历史数据。我的 DAG 设置了 `start_date=2020-01-01, catchup=False`，所以默认不补跑。如果需要，改成 `catchup=True`，Airflow 会用每天的日期参数自动回填历史。

**Q12: Airflow 的 Executor 你了解哪些？**
> 开发用 SequentialExecutor（单进程串行，standalone 默认），生产用 LocalExecutor（多进程并行）或 CeleryExecutor（分布式）。我的项目用 standalone 模式开发就够。

### PostgreSQL

**Q13: 你用了哪些 PostgreSQL 特性？**
> SERIAL 自增主键、NUMERIC 精确小数（金额场景）、TIMESTAMP 时间戳、ON CONFLICT DO UPDATE（UPSERT）、COMMENT ON TABLE（表注释）。都是 PG 相对 MySQL 的优势功能。

**Q14: NUMERIC 和 FLOAT 的区别？为什么金额用 NUMERIC？**
> FLOAT 是近似值，有精度丢失（比如 0.1 + 0.2 ≠ 0.3）。NUMERIC 是精确存储，适合金额。这是财务系统的基本要求。

**Q15: etl_task_logs 表怎么用的？**
> 每个 ETL 步骤开始时 INSERT 一条 running 状态记录，成功则 UPDATE 为 success 并填入 end_time 和 duration，失败则 UPDATE 为 failed 并填入 error_message。可以通过 `SELECT * FROM etl_task_logs WHERE status='failed'` 快速定位问题。

### 项目综合

**Q16: 这个项目最大的挑战是什么？**
> 一是数据清洗规则的设计——需要理解业务含义（C 开头是取消，负数是退货）；二是数仓分层边界的把握——什么逻辑放 DWD、什么放 DWS；三是 Airflow DAG 的容错设计——重试策略、日志记录、任务依赖。

**Q17: 如果要扩展这个项目，你会加什么？**
> 1) 增量抽取而非全量；2) 更多维度表（商品维、客户维、日期维）形成星型模型；3) 数据可视化看板替代 CSV 日报；4) 数据血缘追踪。

**Q18: 这个项目的数据量级是多少？能处理多大的数据？**
> Online Retail 数据集约 54 万行，这是小数据量。如果数据量到千万级，Pandas 全量加载会 OOM。改进方案：1) 用 SQL 做清洗替代 Pandas；2) 分批次处理（chunk read）；3) 引入 Spark。

**Q19: 你的项目代码怎么组织？为什么这么分？**
> 一个 ETL 步骤一个 py 文件，职责单一。db.py 抽离了数据库连接，方便复用。tests/ 下每个模块对应一个测试文件。这是标准的 Python 项目结构，容易维护也方便协作。

**Q20: 测试覆盖了哪些场景？**
> 21 个测试覆盖了：正常数据流、边界情况（空数据、全脏数据）、异常流程（文件不存在、列缺失）、计算正确性（金额公式、聚合去重）。用了 SQLite 内存库替代真实 PG，测试不需要外部依赖。

---

## 八、STAR 面试话术

### S — Situation（场景）

> "我在做一个电商订单数据分析的学习项目，需求是把每天产生的 CSV 订单数据自动化处理成运营日报。"

### T — Task（任务）

> "我的任务是搭建一个完整的离线数据仓库，包含数据抽取、清洗、汇总和报表生成，并且要能每日自动运行。"

### A — Action（行动）

> "首先我设计了 ODS → DWD → DWS 三层数仓架构，明确了每层的职责和数据流转规则。然后用 Python + Pandas 实现了 5 步 ETL 流程，包括数据质量检查和清洗流水线。接着基于 Airflow 搭建了 DAG 调度，配置了任务依赖、失败重试和日志记录。最后写了 21 个 pytest 测试覆盖所有模块。"

### R — Result（结果）

> "最终实现了一个完整可运行的离线数仓系统：每天凌晨自动执行 ETL，产出日度销售报表，包含订单数、客户数、销售额和客单价四个核心指标。代码 900 行，测试全部通过，已经部署到 GitHub。"

---

## 九、面试前快速检查清单 ⭐

- [ ] 能画出项目架构图（Airflow → ETL五步 → 数仓三层）
- [ ] 能说出每层表名和核心字段
- [ ] 能解释为什么过滤了哪些数据（C开头/空客户/非正数）
- [ ] 能讲清楚 DWD 层做了哪 5 步清洗
- [ ] 能解释 UPSERT 和 TRUNCATE 的选择原因
- [ ] 能描述 Airflow DAG 的任务依赖关系
- [ ] 知道怎么查看 ETL 任务历史日志
- [ ] 能运行 `pytest tests/ -v` 展示 21 项全绿
- [ ] 能打开 GitHub README 流畅讲解
- [ ] 准备好 30 秒电梯演讲

---

## 十、项目 GitHub 地址

🔗 **https://github.com/886ss/order-etl-project**

---

*祝你面试顺利！🚀*
