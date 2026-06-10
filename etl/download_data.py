"""
数据集下载工具
==============
从 UCI Machine Learning Repository 下载 Online Retail Dataset。
"""

import os
import urllib.request
import zipfile

# Online Retail Dataset URL (UCI)
DATASET_URL = (
    "https://archive.ics.uci.edu/static/public/352/"
    "online+retail.zip"
)

OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "online_retail.csv")


def download_dataset():
    """下载并解压 Online Retail 数据集"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    zip_path = os.path.join(OUTPUT_DIR, "online_retail.zip")

    if os.path.exists(OUTPUT_FILE):
        print(f"数据集已存在: {OUTPUT_FILE}")
        return

    print(f"正在下载数据集...")
    print(f"URL: {DATASET_URL}")

    try:
        urllib.request.urlretrieve(DATASET_URL, zip_path)
        print(f"下载完成: {zip_path}")

        # 解压
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(OUTPUT_DIR)
        print(f"解压完成: {OUTPUT_DIR}")

        # 重命名 CSV（UCI 解压后文件名可能不同）
        for fname in os.listdir(OUTPUT_DIR):
            if fname.endswith(".xlsx"):
                import pandas as pd
                xlsx_path = os.path.join(OUTPUT_DIR, fname)
                df = pd.read_excel(xlsx_path)
                df.to_csv(OUTPUT_FILE, index=False)
                print(f"已转换: {fname} → {OUTPUT_FILE}")
                break

        # 清理 zip
        os.remove(zip_path)
        print("完成！数据集就绪。")

    except Exception as e:
        print(f"下载失败: {e}")
        print()
        print("请手动下载数据集：")
        print(
            "1. 访问 https://archive.ics.uci.edu/dataset/352/online+retail"
        )
        print(f"2. 下载 Online Retail.xlsx")
        print(f"3. 保存为 {OUTPUT_FILE}")


if __name__ == "__main__":
    download_dataset()
