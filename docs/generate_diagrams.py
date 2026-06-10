"""
项目架构图生成脚本
==================
使用 matplotlib 生成三张架构图：
- architecture.png    项目整体架构
- warehouse_design.png 数仓分层设计
- dag_flow.png         Airflow DAG 流程
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np

plt.rcParams["font.family"] = "sans-serif"
plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


def draw_rounded_box(ax, x, y, w, h, text, color, text_color="white", fontsize=10):
    """绘制圆角矩形"""
    box = FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.15",
        facecolor=color, edgecolor="#333", linewidth=1.5,
    )
    ax.add_patch(box)
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center",
            color=text_color, fontsize=fontsize, fontweight="bold")


def draw_arrow(ax, x1, y1, x2, y2, color="#555"):
    """绘制箭头"""
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="->", color=color, lw=2))


# ============================================================
# 图1: 项目整体架构
# ============================================================
def generate_architecture():
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 8)
    ax.axis("off")
    ax.set_title("订单数据数仓ETL — 项目整体架构", fontsize=16, fontweight="bold", pad=20)

    # 顶部: Airflow
    draw_rounded_box(ax, 3.5, 6.8, 5, 0.7, "Apache Airflow 调度层\n(daily_order_pipeline)", "#017CEE")

    # ETL 核心层
    tasks = [
        ("extract_orders", "#2E86AB"),
        ("check_quality", "#A23B72"),
        ("build_dwd", "#F18F01"),
        ("build_dws", "#C73E1D"),
        ("generate_report", "#3B1F2B"),
    ]
    box_w = 1.8
    start_x = 0.8
    gap = 0.3
    for i, (name, color) in enumerate(tasks):
        x = start_x + i * (box_w + gap)
        draw_rounded_box(ax, x, 4.8, box_w, 0.8, name, color, fontsize=8)
        # Arrow from Airflow
        draw_arrow(ax, 6, 6.8, x + box_w / 2, 5.6)

    # 数仓层
    layers = [
        ("ODS 层 (ods_orders)", "#4CAF50", 0.8),
        ("DWD 层 (dwd_orders)", "#FF9800", 3.8),
        ("DWS 层 (dws_sales_daily)", "#F44336", 6.8),
    ]
    for label, color, x in layers:
        draw_rounded_box(ax, x, 2.8, 2.5, 0.8, label, color)

    # 箭头连接 ETL → 数仓
    draw_arrow(ax, 1.7, 4.8, 2.05, 3.6)
    draw_arrow(ax, 4.3, 4.8, 5.05, 3.6)
    draw_arrow(ax, 7.0, 4.8, 8.05, 3.6)

    # 数据源和输出
    draw_rounded_box(ax, 0.8, 1.0, 2.5, 0.7, "CSV 订单数据", "#607D8B")
    draw_rounded_box(ax, 8.7, 1.0, 2.5, 0.7, "日报输出 (.csv)", "#607D8B")

    draw_arrow(ax, 2.05, 1.7, 2.05, 2.8)
    draw_arrow(ax, 8.05, 2.8, 9.95, 1.7)

    # 图例
    plt.tight_layout()
    fig.savefig("docs/architecture.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("[OK] architecture.png generated")


# ============================================================
# 图2: 数仓分层设计
# ============================================================
def generate_warehouse_design():
    fig, ax = plt.subplots(1, 1, figsize=(14, 8))
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 8)
    ax.axis("off")
    ax.set_title("数仓分层设计 — ODS → DWD → DWS", fontsize=16, fontweight="bold", pad=20)

    # ODS 层
    draw_rounded_box(ax, 1, 5.5, 12, 1.6, "", "#E8F5E9")
    ax.text(7, 6.8, "ODS 层 — ods_orders", fontsize=13, fontweight="bold", ha="center", color="#2E7D32")
    ax.text(7, 6.2, "原始订单数据镜像 | 不做业务处理 | InvoiceNo, StockCode, Quantity, UnitPrice, CustomerID, Country ...",
            fontsize=9, ha="center", color="#555")
    ax.text(7, 5.7, "行数: ~540,000  |  来源: CSV 文件  |  策略: TRUNCATE + INSERT 全量同步",
            fontsize=8, ha="center", color="#888")

    # 箭头
    draw_arrow(ax, 7, 5.5, 7, 4.5)

    # DWD 层
    draw_rounded_box(ax, 1, 3.0, 12, 1.6, "", "#FFF3E0")
    ax.text(7, 4.3, "DWD 层 — dwd_orders", fontsize=13, fontweight="bold", ha="center", color="#E65100")
    ax.text(7, 3.7, "清洗后明细 | 去取消订单 | 去空CustomerID | 去Quantity≤0 | 去UnitPrice≤0 | order_amount = Qty × Price",
            fontsize=9, ha="center", color="#555")
    ax.text(7, 3.2, "新增字段: order_amount(订单金额), etl_time(处理时间)  |  策略: TRUNCATE + INSERT 全量同步",
            fontsize=8, ha="center", color="#888")

    # 箭头
    draw_arrow(ax, 7, 3.0, 7, 2.0)

    # DWS 层
    draw_rounded_box(ax, 1, 0.5, 12, 1.6, "", "#FFEBEE")
    ax.text(7, 1.8, "DWS 层 — dws_sales_daily", fontsize=13, fontweight="bold", ha="center", color="#C62828")
    ax.text(7, 1.2, "日度汇总 | daily_order_count | daily_customer_count | daily_sales_amount | daily_avg_order_amount",
            fontsize=9, ha="center", color="#555")
    ax.text(7, 0.7, "聚合粒度: 按日(DATE)  |  策略: UPSERT (ON CONFLICT UPDATE)",
            fontsize=8, ha="center", color="#888")

    plt.tight_layout()
    fig.savefig("docs/warehouse_design.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("[OK] warehouse_design.png generated")


# ============================================================
# 图3: Airflow DAG 流程图
# ============================================================
def generate_dag_flow():
    fig, ax = plt.subplots(1, 1, figsize=(8, 10))
    ax.set_xlim(0, 8)
    ax.set_ylim(0, 10)
    ax.axis("off")
    ax.set_title("Airflow DAG — daily_order_pipeline", fontsize=16, fontweight="bold", pad=20)

    tasks = [
        ("extract_orders", "数据抽取\nCSV → ODS", "#2E86AB", 8.5),
        ("check_quality", "质量检查\n空值/重复/异常", "#A23B72", 7.0),
        ("build_dwd", "构建DWD层\n清洗+去重+金额", "#F18F01", 5.5),
        ("build_dws", "构建DWS层\n按日聚合指标", "#C73E1D", 4.0),
        ("generate_report", "生成日报\nCSV + 控制台", "#3B1F2B", 2.5),
        ("finish", "完成", "#388E3C", 1.0),
    ]

    for i, (name, desc, color, y) in enumerate(tasks):
        draw_rounded_box(ax, 2.5, y, 3, 1.0, f"{name}\n{desc}", color, fontsize=8)
        if i < len(tasks) - 1:
            # Arrow down
            draw_arrow(ax, 4, y, 4, tasks[i + 1][3] + 1.0)

    # 右侧注释
    ax.text(6.2, 8.5, "[特性]", fontsize=10, fontweight="bold", color="#333")
    features = [
        "- 每日 2:00 自动执行",
        "- 失败重试 3 次",
        "- 重试间隔 5 分钟",
        "- 日志自动记录",
        "- 任务依赖清晰",
    ]
    for i, feat in enumerate(features):
        ax.text(6.2, 8.0 - i * 0.35, feat, fontsize=8, color="#666")

    plt.tight_layout()
    fig.savefig("docs/dag_flow.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("[OK] dag_flow.png generated")


if __name__ == "__main__":
    generate_architecture()
    generate_warehouse_design()
    generate_dag_flow()
    print("\nAll diagrams generated successfully!")
