-- ============================================================
-- 初始化数据脚本
-- 在首次运行 ETL 前执行，确保基础环境就绪
-- ============================================================

-- 清理测试数据（仅开发环境使用）
-- TRUNCATE TABLE ods_orders, dwd_orders, dws_sales_daily, etl_task_logs RESTART IDENTITY;

-- 检查表是否创建成功
SELECT
    table_name,
    (SELECT count(*) FROM information_schema.columns WHERE table_name = t.table_name) AS column_count
FROM information_schema.tables t
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE'
  AND table_name IN ('ods_orders', 'dwd_orders', 'dws_sales_daily', 'etl_task_logs')
ORDER BY table_name;
