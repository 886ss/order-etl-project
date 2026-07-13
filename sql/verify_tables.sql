-- ============================================================
-- 验证表结构脚本（不插入任何数据）
-- 在 CREATE TABLE 后执行，确认所有表正确创建
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
