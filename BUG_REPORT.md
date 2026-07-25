# ETL 项目新数据集兼容性测试报告 (含修复记录)

> 报告日期: 2025-07-25
> 修复日期: 2025-07-25
> 测试: 86 项全部通过 (36 旧 + 50 新)

---

## 一、总体评估：落地能力得分 — 2/10

| 维度 | 得分 | 说明 |
|------|------|------|
| Schema 灵活性 | 0/3 | 列映射硬编码，换数据集必改源码 |
| 数据类型容错 | 1/3 | 假设所有 ID 为字符串，int ID 直接崩溃 |
| 业务逻辑通用性 | 0/2 | 取消订单检测、金额公式均与新数据不兼容 |
| 数据保真度 | 1/2 | 41 列 → 8 列，丢失 80% 字段 |
| **总分** | **2/10** | **换数据集基本不可用，需大量代码修改** |

---

## 二、Critical Bugs（阻塞级 — 3 个）

### 🔴 BUG-001: Schema 硬编码，无法适配不同数据源
- **文件**: [extract.py](order-etl-project/etl/extract.py#L17-L25)
- **严重程度**: Critical (Blocker)
- **现象**: `COLUMN_MAPPING` 是模块级硬编码常量，包含 8 个 UCI 专用列名。新数据集的列名 `Order_ID`、`Product_ID`、`Order_Date`、`Unit_Price`、`Customer_ID` 全部不匹配。
- **对比**:

| ETL 期望列名 | 新数据集列名 | 匹配? |
|-------------|-------------|------|
| InvoiceNo | Order_ID | ❌ |
| StockCode | Product_ID | ❌ |
| Description | (无对应列) | ❌ |
| Quantity | Quantity | ✅ (唯一碰巧匹配) |
| InvoiceDate | Order_Date | ❌ |
| UnitPrice | Unit_Price | ❌ |
| CustomerID | Customer_ID | ❌ |
| Country | Country | ✅ |
- **影响**: `validate_columns()` 直接拒绝数据，无法进入 ODS 加载。
- **修复建议**: 将 `COLUMN_MAPPING` 抽取为 YAML/JSON 配置文件，或提供 `set_column_mapping()` 接口。

### 🔴 BUG-002: int64 列上调用 `.str` 方法直接崩溃
- **文件**: [transform.py](order-etl-project/etl/transform.py#L61)
- **严重程度**: Critical (Crash)
- **现象**: `clean_data()` 第 61 行 `df["invoice_no"].str.startswith("C", na=False)` — 新数据集的 Order_ID 是 **int64**，pandas 的 `.str` accessor 无法处理整型列，抛出 `AttributeError: Can only use .str accessor with string values!`
- **堆栈**:
  ```
  etl\transform.py:61: in clean_data
      cancel_mask = df["invoice_no"].str.startswith("C", na=False)
  pandas\core\strings\accessor.py:194: in __init__
      self._inferred_dtype = self._validate(data)
  AttributeError: Can only use .str accessor with string values!
  ```
- **影响**: 整个 transform 阶段崩溃，后续 aggregate/report 全部无法执行。
- **修复建议**: 在 `.str` 调用前加类型检查：`df["invoice_no"].astype(str).str.startswith("C", na=False)`。

### 🔴 BUG-003: 取消订单检测逻辑对新数据集完全失效
- **文件**: [transform.py](order-etl-project/etl/transform.py#L61)
- **严重程度**: Critical (Data Quality)
- **现象**: ETL 通过 "InvoiceNo 以 C 开头" 检测取消订单（UCI 数据集约定），但新数据集用 `Returned` (Yes/No) 和 `Order_Status` (Delivered/Cancelled/Returned) 列标识。即使修复了 BUG-002 的类型问题，也无法检测到新数据集的退货。
- **影响**: 退货/取消订单混入 DWD 层，销售额被高估。
- **修复建议**: 取消订单检测规则应可配置（列名 + 判定逻辑），而非硬编码。

---

## 三、Major Bugs（严重影响 — 3 个）

### 🟠 BUG-004: 金额计算公式忽略折扣/运费/税
- **文件**: [transform.py](order-etl-project/etl/transform.py#L85)
- **严重程度**: Major (财务数据失真)
- **现象**: ETL 用 `order_amount = Quantity × UnitPrice`，但新数据集有 `Discount_Amount`、`Shipping_Cost`、`Tax_Amount`。正确公式应为 `Order_Amount = Unit_Price × Quantity - Discount_Amount + Shipping_Cost + Tax_Amount`。
- **数据验证**:
  ```
  ETL 简单金额 = Quantity × UnitPrice (不含折扣/运费/税)
  真实订单金额 = 数据集自带的 Order_Amount (含所有调整)
  偏差: 显著（取决于折扣率，数据集中折扣最高达 50%）
  ```
- **影响**: DWS 层销售额指标严重失真，日报的"累计销售额"不可信。

### 🟠 BUG-005: 列映射导致 80% 数据字段丢失
- **文件**: [extract.py](order-etl-project/etl/extract.py#L100)
- **严重程度**: Major (信息丢失)
- **现象**: `load_to_ods()` 只保留 `COLUMN_MAPPING` 中的 8 列，新数据集的 41 列中仅 2 列（Quantity, Country）能直接匹配。丢失的关键字段包括:
  - 财务: Discount_Amount, Shipping_Cost, Tax_Amount, Profit_Amount, Profit_Margin_Percent
  - 客户画像: Customer_Age, Customer_Gender, Customer_Segment, Membership_Status, Customer_Lifetime_Value
  - 产品: Product_Category, Product_Subcategory, Brand
  - 订单: Order_Status, Returned, Payment_Method, Device_Type, Review_Rating
- **影响**: ODS 层丢失大量可用于分析维度，DWD/DWS 分析深度严重受限。

### 🟠 BUG-006: 无 Schema 配置层，扩展性为零
- **文件**: 整个项目
- **严重程度**: Major (架构缺陷)
- **现象**: 项目中不存在任何 JSON/YAML/TOML schema 配置文件。列映射、数据类型、清洗规则、聚合指标全部硬编码在 Python 源码中。适配新数据集需要修改 5+ 个源文件。

---

## 四、Minor Bugs & Warnings（轻微问题 — 2 个）

### 🟡 BUG-007: 缺少 PostgreSQL 则完全无法运行
- **文件**: [db.py](order-etl-project/etl/db.py#L29-L44)
- **严重程度**: Minor (部署障碍)
- **现象**: `get_engine()` 硬编码 PostgreSQL 连接字符串，没有 SQLite fallback。测试用 monkeypatch 绕过，但实际使用需要先安装配置 PostgreSQL。
- **影响**: 无法快速在本地验证，部署门槛高。

### 🟡 BUG-008: 日期格式假设
- **文件**: [extract.py](order-etl-project/etl/extract.py#L101)
- **严重程度**: Minor (兼容性)
- **现象**: UCI 数据日期格式为 `2010-12-01 08:26:00`（含时间），新数据集为 `2023-01-01`（仅日期）。`pd.to_datetime()` 能兼容两者，但如果遇到其他格式（如 `12/01/2010`），需要额外处理。

---

## 五、测试数据汇总

```
测试用例: 24 项
通过: 10 项 (42%)
失败: 14 项 (58%)

通过项分布:
  - CSV 编码自动检测 ✓
  - Schema 校验正确拒绝不匹配列 ✓
  - 硬编码检测 ✓
  - 边界条件（空数据/多国家/单行） ✓

失败项分布:
  - Transform clean_data 崩溃: 8 项 (连锁失败)
  - 数据类型不兼容: 3 项
  - Schema 校验细节: 1 项
  - Aggregate/Report 依赖 Transform: 2 项
```

---

## 六、修复优先级建议

| 优先级 | Bug ID | 修复工作量 | 说明 |
|--------|--------|-----------|------|
| P0 | BUG-002 | 1 行代码 | `.astype(str)` 防止崩溃 |
| P0 | BUG-001 | 中型重构 | Schema 配置化 |
| P1 | BUG-003 | 中型重构 | 业务规则配置化 |
| P1 | BUG-004 | 小型重构 | 金额公式可配置 |
| P2 | BUG-005 | 大型重构 | ODS 动态 schema |
| P2 | BUG-006 | 大型重构 | 整体配置层 |
| P3 | BUG-007 | 小型 | SQLite fallback |
| P3 | BUG-008 | 1 行代码 | 日期格式推断 |

---

## 八、修复记录 (2025-07-25)

### 新增文件

| 文件 | 行数 | 说明 |
|------|------|------|
| `etl/config.py` | 150 | RuntimeSchema 数据类 + YAML 加载 + JSON 缓存 + schema 合并 |
| `etl/sanitizer.py` | 225 | 文件结构修复：编码/分隔符/BOM/表头/尾部行/空列 (8 项检测) |
| `etl/profiler.py` | 280 | 列角色推断：date/numeric/categorical/id/bool/drop (11 项检测) |
| `etl/schema.yaml` | 60 | UCI 默认配置 |
| `data/schema_2026.yaml` | 60 | 2026 新数据集配置 |

### 修改文件

| 文件 | 改动 | 说明 |
|------|------|------|
| `etl/extract.py` | +50 行 | 新增 auto_extract(), _load_to_ods_dynamic() |
| `etl/transform.py` | +170 行 | 新增 clean_data_auto(), auto_transform() — BUG-002 修复 |
| `etl/aggregate.py` | +120 行 | 新增 auto_aggregate(), run_aggregate_auto() |
| `etl/report.py` | +35 行 | 新增 auto_report() — CSV+JSON 双格式 |

### Bug 修复状态

| Bug | 状态 | 修复方式 |
|-----|------|---------|
| BUG-001 (Schema硬编码) | ✅ 已修复 | RuntimeSchema + YAML 配置驱动 |
| BUG-002 (int列.str崩溃) | ✅ 已修复 | clean_data_auto 中 astype(str) 防御 |
| BUG-003 (取消检测失效) | ✅ 已修复 | cancel_rules 可配置 (method/column/value) |
| BUG-004 (金额公式) | ✅ 已修复 | computed_columns 表达式可配置 |
| BUG-005 (字段丢失) | ✅ 已缓解 | 全字段保留 ODS，profiler 自动识别所有列角色 |
| BUG-006 (无配置层) | ✅ 已修复 | schema.yaml + schema_cache.json |
| BUG-007 (缺PG不可用) | ⚠️ 未修复 | 仍需要 PG（设计约束），但测试全用 SQLite |
| BUG-008 (日期格式) | ✅ 已修复 | profiler 自动检测多种日期格式 |

### 落地能力重新评分: 2/10 → 7/10

| 维度 | 修复前 | 修复后 | 提升 |
|------|--------|--------|------|
| Schema 灵活性 | 0/3 | 2/3 | YAML驱动，但列角色推断仍需训练 |
| 数据类型容错 | 1/3 | 3/3 | astype(str) 防御 + profiler 自动检测 |
| 业务逻辑通用性 | 0/2 | 1/2 | 规则可配置，但跨列交叉校验待加强 |
| 数据保真度 | 1/2 | 1/2 | 全字段保留，但聚合策略仍需调优 |

剩余 3 分留给: PostgreSQL 抽象层、交叉列校验、增量对比报告。
