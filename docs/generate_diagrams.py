"""
项目架构图生成脚本 v2.0
======================
使用 matplotlib 生成三张架构图：
- architecture.png    项目整体架构（S→P→Q→T→A→R 六层管道）
- warehouse_design.png 数仓分层设计（动态 schema）
- dag_flow.png         Airflow DAG 流程
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.font_manager as fm
from matplotlib.patches import FancyBboxPatch
import numpy as np

def _detect_chinese_font() -> list:
    available = {f.name for f in fm.fontManager.ttflist}
    candidates = [
        "SimHei", "Microsoft YaHei", "PingFang SC",
        "Heiti SC", "Noto Sans CJK SC", "WenQuanYi Micro Hei", "DejaVu Sans",
    ]
    detected = [f for f in candidates if f in available]
    return detected if detected else ["sans-serif"]

plt.rcParams["font.family"] = "sans-serif"
plt.rcParams["font.sans-serif"] = _detect_chinese_font()
plt.rcParams["axes.unicode_minus"] = False


def draw_rounded_box(ax, x, y, w, h, text, color, text_color="white", fontsize=10):
    box = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.15",
                         facecolor=color, edgecolor="#333", linewidth=1.5)
    ax.add_patch(box)
    ax.text(x + w/2, y + h/2, text, ha="center", va="center",
            color=text_color, fontsize=fontsize, fontweight="bold")


def draw_arrow(ax, x1, y1, x2, y2, color="#555"):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="->", color=color, lw=2))


# ============================================================
# 图1: 项目整体架构 v2.0
# ============================================================
def generate_architecture():
    fig, ax = plt.subplots(1, 1, figsize=(14, 9))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 9)
    ax.axis("off")
    ax.set_title("Order Data Warehouse ETL v2.0 — Architecture", fontsize=14, fontweight="bold", pad=15)

    # === TOP: Airflow Scheduler ===
    draw_rounded_box(ax, 4.5, 8.0, 5, 0.6, "Apache Airflow (daily_order_pipeline)", "#017CEE", fontsize=9)

    # === MIDDLE: 6-layer pipeline S→P→Q→T→A→R ===
    layers = [
        ("S", "sanitizer.py\nFile Repair", "#2E7D32"),
        ("P", "profiler.py\nColumn Infer", "#1565C0"),
        ("Q", "quality.py\nQuality Scan", "#6A1B9A"),
        ("T", "transform.py\nClean Engine", "#E65100"),
        ("A", "aggregate.py\nAuto Aggregate", "#C62828"),
        ("R", "report.py\nCSV + JSON", "#37474F"),
    ]
    box_w = 1.8
    gap = 0.2
    start_x = 1.0
    for i, (abbr, desc, color) in enumerate(layers):
        x = start_x + i * (box_w + gap)
        draw_rounded_box(ax, x, 5.5, box_w, 1.2, f"{abbr}\n{desc}", color, fontsize=7.5)
        draw_arrow(ax, 7.0, 8.0, x + box_w/2, 6.7)

    # Arrow chain between layers
    for i in range(len(layers) - 1):
        x1 = start_x + i * (box_w + gap) + box_w
        x2 = start_x + (i+1) * (box_w + gap)
        draw_arrow(ax, x1, 6.1, x2, 6.1)

    # === BOTTOM: Data layers ===
    draw_rounded_box(ax, 1.0, 3.2, 3.7, 0.7, "ODS 层\n(动态建表, 全字段)", "#4CAF50", fontsize=8)
    draw_rounded_box(ax, 5.15, 3.2, 3.7, 0.7, "DWD 层\n(通用清洗, 脏数据归档)", "#FF9800", fontsize=8)
    draw_rounded_box(ax, 9.3, 3.2, 3.7, 0.7, "DWS 层\n(自适应聚合, 日期x数值x分类)", "#F44336", fontsize=8)

    draw_arrow(ax, 2.85, 5.5, 2.85, 3.9)
    draw_arrow(ax, 6.0, 4.4, 7.0, 3.9)
    draw_arrow(ax, 9.0, 4.4, 11.15, 3.9)

    # === INPUT / OUTPUT ===
    draw_rounded_box(ax, 1.0, 1.5, 3.7, 0.8, "Input\nany CSV (auto-detect)", "#607D8B", fontsize=8)
    draw_rounded_box(ax, 9.3, 1.5, 3.7, 0.8, "Output\nCSV Report + JSON Summary", "#607D8B", fontsize=8)
    draw_arrow(ax, 2.85, 2.3, 2.85, 3.2)
    draw_arrow(ax, 11.15, 3.2, 11.15, 2.3)

    # === RIGHT: new modules ===
    ax.text(13.0, 7.5, "[New v2.0]", fontsize=10, fontweight="bold", color="#1565C0")
    new_mods = ["config.py", "sanitizer.py", "profiler.py"]
    for i, m in enumerate(new_mods):
        ax.text(13.0, 7.0 - i*0.35, f"+ {m}", fontsize=8, color="#1565C0")

    plt.tight_layout()
    fig.savefig("docs/architecture.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("[OK] architecture.png")


# ============================================================
# 图2: 数仓分层设计 v2.0
# ============================================================
def generate_warehouse_design():
    fig, ax = plt.subplots(1, 1, figsize=(14, 8))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 8)
    ax.axis("off")
    ax.set_title("Data Warehouse Layers — Auto Schema Inference", fontsize=14, fontweight="bold", pad=20)

    # ODS
    draw_rounded_box(ax, 1, 5.5, 12, 1.5, "", "#E8F5E9")
    ax.text(7, 6.7, "ODS — ods_orders (dynamic)", fontsize=12, fontweight="bold", ha="center", color="#2E7D32")
    ax.text(7, 6.2, "Raw mirror | auto-encoding | auto-separator | BOM strip | all columns preserved",
            fontsize=9, ha="center", color="#555")
    ax.text(7, 5.7, "sanitizer.read_csv_safe() → profile columns → dynamic_load('ods_orders')",
            fontsize=8, ha="center", color="#888")

    draw_arrow(ax, 7, 5.5, 7, 4.5)

    # DWD
    draw_rounded_box(ax, 1, 3.0, 12, 1.5, "", "#FFF3E0")
    ax.text(7, 4.2, "DWD — dwd_orders (auto_transform)", fontsize=12, fontweight="bold", ha="center", color="#E65100")
    ax.text(7, 3.7, "General clean engine | cancel/required/validation/computed rules | rejected archive",
            fontsize=9, ha="center", color="#555")
    ax.text(7, 3.2, "clean_data_auto(df, schema) → dynamic_load('dwd_orders')  |  BUG-002: lazy astype(str)",
            fontsize=8, ha="center", color="#888")

    draw_arrow(ax, 7, 3.0, 7, 2.0)

    # DWS
    draw_rounded_box(ax, 1, 0.5, 12, 1.5, "", "#FFEBEE")
    ax.text(7, 1.7, "DWS — dws_sales_daily (auto_aggregate)", fontsize=12, fontweight="bold", ha="center", color="#C62828")
    ax.text(7, 1.2, "Date dims x Numeric metrics(SUM+AVG) x Category dims(COUNT) | ID-columns excluded",
            fontsize=9, ha="center", color="#555")
    ax.text(7, 0.7, "auto_aggregate(df, schema) → dynamic_load('dws_sales_daily')  |  UPSERT on conflict",
            fontsize=8, ha="center", color="#888")

    # Config box
    draw_rounded_box(ax, 0.3, 7.2, 2.5, 0.6, "schema.yaml\n(optional override)", "#1565C0", fontsize=7)

    plt.tight_layout()
    fig.savefig("docs/warehouse_design.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("[OK] warehouse_design.png")


# ============================================================
# 图3: Airflow DAG 流程图
# ============================================================
def generate_dag_flow():
    fig, ax = plt.subplots(1, 1, figsize=(8, 10))
    ax.set_xlim(0, 8)
    ax.set_ylim(0, 10)
    ax.axis("off")
    ax.set_title("Airflow DAG — daily_order_pipeline", fontsize=14, fontweight="bold", pad=20)

    tasks = [
        ("extract_orders", "Extract\nCSV → ODS", "#2E86AB", 8.5),
        ("check_quality", "Quality\nNull/Dup/Outlier", "#6A1B9A", 7.0),
        ("build_dwd", "Transform\nClean + Archive", "#F18F01", 5.5),
        ("build_dws", "Aggregate\nDate×Num×Cat", "#C73E1D", 4.0),
        ("generate_report", "Report\nCSV + JSON", "#3B1F2B", 2.5),
        ("finish", "Done", "#388E3C", 1.0),
    ]

    for i, (name, desc, color, y) in enumerate(tasks):
        draw_rounded_box(ax, 2.5, y, 3, 1.0, f"{name}\n{desc}", color, fontsize=8)
        if i < len(tasks) - 1:
            draw_arrow(ax, 4, y, 4, tasks[i+1][3] + 1.0)

    ax.text(6.2, 8.5, "[Config]", fontsize=10, fontweight="bold", color="#333")
    features = [
        "- Daily 2:00 UTC",
        "- 3x retry / 5min",
        "- on_failure_callback",
        "- Auto-log to DB",
        "- Full/Incremental",
    ]
    for i, feat in enumerate(features):
        ax.text(6.2, 8.0 - i*0.35, feat, fontsize=8, color="#666")

    plt.tight_layout()
    fig.savefig("docs/dag_flow.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("[OK] dag_flow.png")


if __name__ == "__main__":
    generate_architecture()
    generate_warehouse_design()
    generate_dag_flow()
    print("\nAll diagrams generated!")
