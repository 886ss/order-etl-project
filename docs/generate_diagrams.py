"""
项目架构图生成脚本 v2.1
======================
生成 7 张图表：
  中文架构系列:
    - architecture_cn.png       项目整体架构（中文版）
    - warehouse_design_cn.png   数仓分层设计（中文版）
    - dag_flow_cn.png           Airflow DAG 流程（中文版）
  数据集示例系列:
    - profile_example.png       列推断结果（2026 数据集）
    - pipeline_summary.png      全链路汇总指标
    - daily_trend.png           日度销售额趋势
    - report_preview.png        日报输出预览
"""

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.patches import FancyBboxPatch
import numpy as np
import pandas as pd

# ── 中文字体 ──
_FONT = "SimHei" if "SimHei" in {f.name for f in fm.fontManager.ttflist} else "sans-serif"
plt.rcParams["font.family"] = "sans-serif"
plt.rcParams["font.sans-serif"] = [_FONT, "Microsoft YaHei", "DejaVu Sans"]
plt.rcParams["font.monospace"] = [_FONT, "Courier New"]  # 中文等宽回退到 SimHei
plt.rcParams["axes.unicode_minus"] = False

# ── 配色 ──
C = {
    "blue":   "#017CEE", "green":  "#2E7D32", "purple": "#6A1B9A",
    "orange": "#E65100", "red":    "#C62828", "gray":   "#37474F",
    "light_green": "#E8F5E9", "light_orange": "#FFF3E0", "light_red": "#FFEBEE",
    "airflow": "#017CEE", "slate": "#607D8B",
}


def box(ax, x, y, w, h, text, color, tc="white", fs=10):
    b = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.15",
                       facecolor=color, edgecolor="#333", linewidth=1.5)
    ax.add_patch(b)
    ax.text(x + w/2, y + h/2, text, ha="center", va="center",
            color=tc, fontsize=fs, fontweight="bold")


def arrow(ax, x1, y1, x2, y2, c="#555"):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="->", color=c, lw=2))


# ================================================================
# 图1: 项目整体架构（中文版）
# ================================================================
def gen_architecture_cn():
    fig, ax = plt.subplots(figsize=(15, 9.5))
    ax.set_xlim(0, 15); ax.set_ylim(0, 9.5); ax.axis("off")
    ax.set_title("订单数据仓库 ETL — 整体架构 (v2.0)", fontsize=15, fontweight="bold", pad=18)

    # 顶部调度层
    box(ax, 4.5, 8.5, 6, 0.6, "Apache Airflow 调度层 (daily_order_pipeline)", C["airflow"], fs=9)

    # 六层管道
    layers = [
        ("S", "文件修复\n编码/分隔符/BOM", C["green"]),
        ("P", "列推断\ndate/num/cat/id",  C["blue"]),
        ("Q", "质量检查\n空值/重复/离群",  C["purple"]),
        ("T", "通用清洗\n规则引擎",         C["orange"]),
        ("A", "自适应聚合\n日期×数值×分类", C["red"]),
        ("R", "报告生成\nCSV + JSON",       C["gray"]),
    ]
    bw, gap, sx = 2.0, 0.15, 0.8
    for i, (abbr, desc, color) in enumerate(layers):
        x = sx + i*(bw+gap)
        box(ax, x, 5.8, bw, 1.2, f"{abbr} 层\n{desc}", color, fs=8)
        arrow(ax, 7.5, 8.5, x+bw/2, 7.0, "#999")

    # 层间箭头
    for i in range(len(layers)-1):
        arrow(ax, sx + i*(bw+gap) + bw, 6.4, sx + (i+1)*(bw+gap), 6.4, "#999")

    # 数仓三层
    box(ax, 0.8, 3.5, 4.2, 0.8, "ODS 层\n动态建表 · 全字段保留",    C["green"], fs=8)
    box(ax, 5.4, 3.5, 4.2, 0.8, "DWD 层\n通用清洗 · 脏数据归档",    C["orange"], fs=8)
    box(ax, 10.0, 3.5, 4.2, 0.8, "DWS 层\n自适应聚合 · 日期×数值×分类", C["red"], fs=8)
    arrow(ax, 2.9, 5.8, 2.9, 4.3)
    arrow(ax, 6.5, 4.8, 7.5, 4.3)
    arrow(ax, 10.2, 4.8, 12.1, 4.3)

    # 输入输出
    box(ax, 0.8, 1.6, 4.2, 0.8, "输入\n任意 CSV → 自动检测编码与结构", C["slate"], fs=8)
    box(ax, 10.0, 1.6, 4.2, 0.8, "输出\nCSV 报告 + JSON 摘要 + 质量报告", C["slate"], fs=8)
    arrow(ax, 2.9, 2.4, 2.9, 3.5)
    arrow(ax, 12.1, 3.5, 12.1, 2.4)

    # v2.0 新增标注
    ax.text(13.5, 8.0, "v2.0 新增", fontsize=9, fontweight="bold", color=C["blue"])
    for i, m in enumerate(["config.py", "sanitizer.py", "profiler.py"]):
        ax.text(13.5, 7.5 - i*0.35, f"+ {m}", fontsize=8, color=C["blue"])

    plt.tight_layout()
    fig.savefig("docs/architecture_cn.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("[OK] architecture_cn.png")


# ================================================================
# 图2: 数仓分层设计（中文版）
# ================================================================
def gen_warehouse_cn():
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.set_xlim(0, 14); ax.set_ylim(0, 8); ax.axis("off")
    ax.set_title("数仓分层设计 — 结构驱动 · 零行业假设", fontsize=14, fontweight="bold", pad=20)

    # ODS
    box(ax, 1, 5.5, 12, 1.5, "", C["light_green"])
    ax.text(7, 6.7, "ODS — ods_orders (动态建表)", fontsize=12, fontweight="bold", ha="center", color=C["green"])
    ax.text(7, 6.2, "原始数据镜像 | 自动编码检测 | 自动分隔符识别 | BOM 剥离 | 所有字段全保留",
            fontsize=9, ha="center", color="#555")
    ax.text(7, 5.7, "sanitizer.read_csv_safe() → profiler.profile_dataframe() → dynamic_load('ods_orders')",
            fontsize=8.5, ha="center", color="#888")

    arrow(ax, 7, 5.5, 7, 4.55)

    # DWD
    box(ax, 1, 3.05, 12, 1.5, "", C["light_orange"])
    ax.text(7, 4.25, "DWD — dwd_orders (通用清洗引擎)", fontsize=12, fontweight="bold", ha="center", color=C["orange"])
    ax.text(7, 3.75, "取消/退货检测 · 必填列过滤 · 值域校验 · 计算列 · 脏数据归档",
            fontsize=9, ha="center", color="#555")
    ax.text(7, 3.3, "clean_data_auto(df, schema) → dynamic_load('dwd_orders')",
            fontsize=8.5, ha="center", color="#888")

    arrow(ax, 7, 3.05, 7, 2.1)

    # DWS
    box(ax, 1, 0.6, 12, 1.5, "", C["light_red"])
    ax.text(7, 1.8, "DWS — dws_sales_daily (自适应聚合)", fontsize=12, fontweight="bold", ha="center", color=C["red"])
    ax.text(7, 1.3, "日期列→时间维度 · 数值列→SUM+AVG · 分类列→COUNT DISTINCT · ID列自动排除",
            fontsize=9, ha="center", color="#555")
    ax.text(7, 0.85, "auto_aggregate(df, schema) → dynamic_load('dws_sales_daily') | UPSERT ON CONFLICT",
            fontsize=8.5, ha="center", color="#888")

    # 数据治理
    dsb = "#F3E5F5"
    ax.add_patch(FancyBboxPatch((1, 0.6), 3, -0.5, boxstyle="round,pad=0.1",
                                facecolor=dsb, edgecolor="#CE93D8", linewidth=1, linestyle="--"))
    ax.text(2.5, 0.35, "脏数据回收站: ods_orders_rejected", fontsize=7, ha="center", color="#6A1B9A")

    plt.tight_layout()
    fig.savefig("docs/warehouse_design_cn.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("[OK] warehouse_design_cn.png")


# ================================================================
# 图3: Airflow DAG 流程图（中文版）
# ================================================================
def gen_dag_flow_cn():
    fig, ax = plt.subplots(figsize=(8, 10))
    ax.set_xlim(0, 8); ax.set_ylim(0, 10); ax.axis("off")
    ax.set_title("Airflow 调度流程 — daily_order_pipeline", fontsize=14, fontweight="bold", pad=20)

    tasks = [
        ("extract_orders",       "数据抽取\nCSV → ODS",         C["blue"],   8.5),
        ("check_quality",        "质量检查\n空值/重复/离群",     C["purple"], 7.0),
        ("build_dwd",            "清洗转换\n规则引擎+脏数据归档", C["orange"], 5.5),
        ("build_dws",            "日度聚合\n日期×数值×分类",     C["red"],    4.0),
        ("generate_report",      "生成报告\nCSV + JSON",        C["gray"],   2.5),
        ("finish",               "流程结束",                    C["green"],  1.0),
    ]
    for i, (name, desc, color, y) in enumerate(tasks):
        box(ax, 2.5, y, 3.2, 1.0, f"{name}\n{desc}", color, fs=8)
        if i < len(tasks)-1:
            arrow(ax, 4.1, y, 4.1, tasks[i+1][3]+1.0)

    ax.text(6.3, 8.5, "调度配置", fontsize=10, fontweight="bold", color="#333")
    for i, f in enumerate([
        "• 每日凌晨 2:00 (UTC)", "• 失败重试 3 次",
        "• 重试间隔 5 分钟", "• 自动告警(微信/飞书/邮件)",
        "• 全量/增量双模式",
    ]):
        ax.text(6.3, 7.9 - i*0.4, f, fontsize=8, color="#666")

    plt.tight_layout()
    fig.savefig("docs/dag_flow_cn.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("[OK] dag_flow_cn.png")


# ================================================================
# 图4: 列推断结果示例（2026 数据集）
# ================================================================
def gen_profile_example():
    csv_path = "data/ecommerce_orders_2026.csv"
    if not os.path.exists(csv_path):
        print("[SKIP] profile_example.png — 2026 数据集不存在")
        return

    from etl.sanitizer import read_csv_safe
    from etl.profiler import profile_dataframe

    df = read_csv_safe(csv_path)
    schema = profile_dataframe(df)

    categories = ["日期列", "数值列", "分类列", "ID 列", "布尔列"]
    counts = [
        len(schema.date_columns), len(schema.numeric_columns),
        len(schema.categorical_columns), len(schema.id_columns), len(schema.bool_columns),
    ]
    colors = ["#1565C0", "#2E7D32", "#E65100", "#C62828", "#6A1B9A"]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.5))
    fig.suptitle("列推断结果 — E-Commerce Orders Dataset 2026", fontsize=14, fontweight="bold", y=0.98)

    # 饼图
    wedges, texts, autotexts = ax1.pie(
        counts, labels=categories, colors=colors, autopct="%1.1f%%",
        startangle=90, pctdistance=0.6, labeldistance=1.12,
    )
    for t in autotexts: t.set_fontsize(10); t.set_fontweight("bold")
    ax1.set_title(f"共 {sum(counts)} 列", fontsize=11, pad=10)

    # 柱状图
    bars = ax2.bar(categories, counts, color=colors, edgecolor="#333", linewidth=0.8, width=0.55)
    for b, v in zip(bars, counts):
        ax2.text(b.get_x()+b.get_width()/2, b.get_height()+1.2, str(v), ha="center", fontsize=14, fontweight="bold")
    ax2.set_ylabel("列数", fontsize=11)
    ax2.set_ylim(0, max(counts)+6)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    ax2.set_title("各角色列数", fontsize=11, pad=10)

    # 关键列名标注
    info_lines = [
        f"日期列: {schema.date_columns}",
        f"ID 列:   {schema.id_columns}",
    ]
    fig.text(0.08, 0.02, "\n".join(info_lines), fontsize=8, color="#666", family="monospace")

    plt.tight_layout(rect=[0, 0.08, 1, 0.94])
    fig.savefig("docs/profile_example.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("[OK] profile_example.png")


# ================================================================
# 图5: 全链路汇总指标（2026 数据集）
# ================================================================
def gen_pipeline_summary():
    csv_path = "data/ecommerce_orders_2026.csv"
    if not os.path.exists(csv_path):
        print("[SKIP] pipeline_summary.png — 2026 数据集不存在")
        return

    from etl.sanitizer import read_csv_safe
    from etl.profiler import profile_dataframe, infer_and_clean_dates
    from etl.transform import clean_data_auto
    from etl.aggregate import auto_aggregate

    df = read_csv_safe(csv_path)
    schema = profile_dataframe(df)
    df = infer_and_clean_dates(df, schema.date_columns)
    df_clean, rejected = clean_data_auto(df, schema)
    agg = auto_aggregate(df_clean, schema)

    result = agg["result"]
    # Key metrics
    metrics = {
        "总行数": f"{len(df_clean):,}",
        "剔除行数": f"{len(rejected):,}",
        "日期范围": f"{result['_stat_date'].min()} ~ {result['_stat_date'].max()}"
                    if "_stat_date" in result.columns else "N/A",
        "聚合天数": f"{len(result):,}",
        "数值指标数": f"{sum(1 for c in result.columns if '_sum' in c or '_avg' in c)}",
    }

    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.axis("off")
    ax.set_xlim(0, 10); ax.set_ylim(0, 4.5)
    ax.set_title("ETL 全链路运行结果 — E-Commerce Orders 2026 (30K 行)", fontsize=14, fontweight="bold", pad=15)

    y = 3.5
    for i, (k, v) in enumerate(metrics.items()):
        if i == 3: y -= 0.1
        ax.text(1, y, f"{k}:", fontsize=12, fontweight="bold", color="#333")
        ax.text(4.5, y, str(v), fontsize=12, color=C["blue"])
        y -= 0.65

    # 步骤耗时标注
    ax.text(1, 0.7, "处理步骤:", fontsize=10, fontweight="bold", color="#555")
    steps = [
        "sanitize → read_csv_safe()  编码自动检测/分隔符嗅探/BOM剥离",
        "profile  → profile_dataframe()  41列 → 日期1/数值17/分类18/ID1/布尔4",
        "clean    → clean_data_auto()  30,000行 → 30,000行 (合成数据无脏行)",
        "aggregate → auto_aggregate()  日期维 × 数值指标 × 分类维 = 1,461天",
        "report   → auto_report()  输出 CSV + JSON 双格式",
    ]
    for i, s in enumerate(steps):
        ax.text(1, 0.35 - i*0.25, s, fontsize=7.5, color="#888", family="monospace")

    plt.tight_layout()
    fig.savefig("docs/pipeline_summary.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("[OK] pipeline_summary.png")


# ================================================================
# 图6: 日度销售额趋势
# ================================================================
def gen_daily_trend():
    csv_path = "data/ecommerce_orders_2026.csv"
    if not os.path.exists(csv_path):
        print("[SKIP] daily_trend.png — 2026 数据集不存在")
        return

    from etl.sanitizer import read_csv_safe
    from etl.profiler import profile_dataframe, infer_and_clean_dates
    from etl.transform import clean_data_auto
    from etl.aggregate import auto_aggregate

    df = read_csv_safe(csv_path)
    schema = profile_dataframe(df)
    df = infer_and_clean_dates(df, schema.date_columns)
    df_clean, _ = clean_data_auto(df, schema)
    agg = auto_aggregate(df_clean, schema)

    result = agg["result"]
    if "_stat_date" not in result.columns:
        print("[SKIP] daily_trend.png — 无日期列")
        return

    result = result.sort_values("_stat_date")

    # 找第一个 sum 列
    sum_cols = [c for c in result.columns if "_sum" in c]
    if not sum_cols:
        print("[SKIP] daily_trend.png — 无 SUM 列")
        return

    # 按月聚合
    result["_month"] = pd.to_datetime(result["_stat_date"]).dt.to_period("M")
    monthly = result.groupby("_month")[sum_cols[0]].sum().reset_index()
    monthly["_month"] = monthly["_month"].astype(str)

    fig, ax = plt.subplots(figsize=(13, 5))
    ax.fill_between(range(len(monthly)), monthly[sum_cols[0]], alpha=0.3, color=C["blue"])
    ax.plot(range(len(monthly)), monthly[sum_cols[0]], color=C["blue"], linewidth=2, marker="o", markersize=4)

    # 标注
    ticks = range(0, len(monthly), max(1, len(monthly)//12))
    ax.set_xticks(ticks)
    ax.set_xticklabels([monthly.iloc[t]["_month"] for t in ticks], rotation=45, fontsize=8)
    ax.set_ylabel(sum_cols[0].replace("_sum", " (SUM)"), fontsize=11)
    ax.set_title("月度趋势 — E-Commerce Orders 2026", fontsize=14, fontweight="bold")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.3)

    # 峰值/谷值标注
    peak_idx = monthly[sum_cols[0]].idxmax()
    valley_idx = monthly[sum_cols[0]].idxmin()
    ax.annotate(f"峰值 {monthly.iloc[peak_idx][sum_cols[0]]:,.0f}",
                xy=(peak_idx, monthly.iloc[peak_idx][sum_cols[0]]),
                xytext=(peak_idx, monthly.iloc[peak_idx][sum_cols[0]]*1.1),
                fontsize=9, color=C["red"], fontweight="bold",
                arrowprops=dict(arrowstyle="->", color=C["red"]))
    ax.annotate(f"谷值 {monthly.iloc[valley_idx][sum_cols[0]]:,.0f}",
                xy=(valley_idx, monthly.iloc[valley_idx][sum_cols[0]]),
                xytext=(valley_idx, monthly.iloc[valley_idx][sum_cols[0]]*0.85),
                fontsize=9, color=C["green"], fontweight="bold",
                arrowprops=dict(arrowstyle="->", color=C["green"]))

    plt.tight_layout()
    fig.savefig("docs/daily_trend.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("[OK] daily_trend.png")


# ================================================================
# 图7: 日报输出预览
# ================================================================
def gen_report_preview():
    from etl.report import save_report, generate_summary
    import tempfile

    df_fake = pd.DataFrame({
        "stat_date":        ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
        "daily_order_count":[120, 145, 98, 167, 134],
        "daily_customer_count":[89, 102, 71, 115, 97],
        "daily_sales_amount":[5230.50, 6890.00, 4120.75, 7450.20, 5890.30],
        "daily_avg_order_amount":[43.59, 47.52, 42.05, 44.61, 43.96],
    })

    with tempfile.TemporaryDirectory() as tmp:
        summary = generate_summary(df_fake)
        path = save_report(df_fake, summary, tmp)

        with open(path, "r", encoding="utf-8") as f:
            content = f.read(1500)

    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.axis("off")
    ax.set_xlim(0, 11); ax.set_ylim(0, 5.5)
    ax.set_title("日报输出预览 — data_report_YYYYMMDD.csv", fontsize=14, fontweight="bold", pad=15)

    # 模拟终端输出
    ax.text(0.3, 4.8, "$ cat reports/data_report_20260725.csv", fontsize=9, color="white",
            bbox=dict(boxstyle="round", facecolor="#333", edgecolor="none", pad=0.4),
            family="monospace")

    lines = content.split("\n")[:18]
    for i, line in enumerate(lines):
        if not line: continue
        color = "#E65100" if line.startswith("#") else "#2E7D32" if "," in line else "#555"
        fs = 8.5 if line.startswith("#") else 8
        ax.text(0.3, 4.2 - i*0.22, line[:95], fontsize=fs, color=color, family="monospace")

    # 右侧说明
    ax.text(8.9, 4.0, "输出格式", fontsize=9, fontweight="bold", color="#333")
    items = [
        "• # 注释头 = 生成时间 + 汇总指标",
        "• CSV 正文 = DWS/聚合结果",
        "• 同目录生成 JSON 摘要",
        "• 可直接导入 Excel / BI 工具",
    ]
    for i, item in enumerate(items):
        ax.text(8.9, 3.5 - i*0.3, item, fontsize=7.5, color="#666")

    plt.tight_layout()
    fig.savefig("docs/report_preview.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("[OK] report_preview.png")


# ================================================================
if __name__ == "__main__":
    gen_architecture_cn()
    gen_warehouse_cn()
    gen_dag_flow_cn()
    gen_profile_example()
    gen_pipeline_summary()
    gen_daily_trend()
    gen_report_preview()
    print("\n全部图表生成完毕!")
