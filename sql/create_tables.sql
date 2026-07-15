-- ============================================================
-- 订单数据数仓 ETL 项目 — 建表脚本
-- 数据库: order_warehouse
-- ============================================================

-- ODS 层: 原始订单数据镜像
CREATE TABLE IF NOT EXISTS ods_orders (
    id              SERIAL PRIMARY KEY,
    invoice_no      VARCHAR(50),
    stock_code      VARCHAR(50),
    description     TEXT,
    quantity        INTEGER,
    invoice_date    TIMESTAMP,
    unit_price      NUMERIC(10, 4),
    customer_id     VARCHAR(50),
    country         VARCHAR(100)
);

COMMENT ON TABLE ods_orders IS 'ODS层-原始订单数据镜像 来源: CSV文件(online_retail.csv)';

-- 脏数据归档表: 记录清洗过程中被剔除的数据
CREATE TABLE IF NOT EXISTS ods_orders_rejected (
    id              SERIAL PRIMARY KEY,
    invoice_no      VARCHAR(50),
    stock_code      VARCHAR(50),
    description     TEXT,
    quantity        INTEGER,
    invoice_date    TIMESTAMP,
    unit_price      NUMERIC(10, 4),
    customer_id     VARCHAR(50),
    country         VARCHAR(100),
    rejected_reason VARCHAR(200)    NOT NULL,
    rejected_at     TIMESTAMP       NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE ods_orders_rejected IS '数据治理-脏数据回收站 来源: transform.clean_data() 剔除行';

-- DWD 层: 清洗后的订单明细
CREATE TABLE IF NOT EXISTS dwd_orders (
    id              SERIAL PRIMARY KEY,
    invoice_no      VARCHAR(50)     NOT NULL,
    stock_code      VARCHAR(50)     NOT NULL,
    description     TEXT,
    quantity        INTEGER         NOT NULL,
    invoice_date    TIMESTAMP       NOT NULL,
    unit_price      NUMERIC(10, 4)  NOT NULL,
    customer_id     VARCHAR(50)     NOT NULL,
    country         VARCHAR(100)    NOT NULL,
    order_amount    NUMERIC(12, 4)  NOT NULL,
    etl_time        TIMESTAMP       NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE dwd_orders IS 'DWD层-清洗后订单明细 来源: ODS (ods_orders) 经 transform.clean_data() 清洗';

-- DWS 层: 销售主题日度汇总
CREATE TABLE IF NOT EXISTS dws_sales_daily (
    id                      SERIAL PRIMARY KEY,
    stat_date               DATE            NOT NULL UNIQUE,
    daily_order_count       INTEGER         NOT NULL DEFAULT 0,
    daily_customer_count    INTEGER         NOT NULL DEFAULT 0,
    daily_sales_amount      NUMERIC(14, 4)  NOT NULL DEFAULT 0,
    daily_avg_order_amount  NUMERIC(12, 4)  NOT NULL DEFAULT 0,
    etl_time                TIMESTAMP       NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE dws_sales_daily IS 'DWS层-销售主题日度汇总 来源: DWD (dwd_orders) 经 aggregate.aggregate_daily() 聚合';

-- ETL 任务日志表
CREATE TABLE IF NOT EXISTS etl_task_logs (
    id              SERIAL PRIMARY KEY,
    task_name       VARCHAR(100)    NOT NULL,
    status          VARCHAR(20)     NOT NULL,
    start_time      TIMESTAMP       NOT NULL,
    end_time        TIMESTAMP,
    duration        NUMERIC(10, 2),
    error_message   TEXT,
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE etl_task_logs IS 'ETL任务执行日志';

-- ============================================================
-- 查询性能索引
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_ods_invoice_date ON ods_orders(invoice_date);
CREATE INDEX IF NOT EXISTS idx_dwd_invoice_date ON dwd_orders(invoice_date);
CREATE INDEX IF NOT EXISTS idx_dwd_customer_id  ON dwd_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_dwd_invoice_no   ON dwd_orders(invoice_no);
