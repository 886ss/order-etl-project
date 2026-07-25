"""
数据集下载工具
==============
从 UCI Machine Learning Repository 下载 Online Retail Dataset。
"""

import os
import urllib.request
import shutil
import zipfile
import logging

logger = logging.getLogger(__name__)

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
        logger.info("数据集已存在: %s", OUTPUT_FILE)
        return

    logger.info("正在下载数据集: %s", DATASET_URL)

    try:
        # 使用 urlopen + copyfileobj 替代已废弃的 urlretrieve
        with urllib.request.urlopen(DATASET_URL) as response, \
             open(zip_path, "wb") as out:
            shutil.copyfileobj(response, out)
        logger.info("下载完成: %s", zip_path)

        # 解压并查找数据文件（优先 .xlsx，其次 .csv）
        # Zip Slip 防护：逐文件校验路径不越界
        output_real = os.path.realpath(OUTPUT_DIR)
        with zipfile.ZipFile(zip_path, "r") as zf:
            for member in zf.infolist():
                dest = os.path.realpath(os.path.join(OUTPUT_DIR, member.filename))
                if not dest.startswith(output_real + os.sep):
                    logger.warning("Zip Slip 跳过: %s", member.filename)
                    continue
                zf.extract(member, OUTPUT_DIR)
        logger.info("解压完成: %s", OUTPUT_DIR)

        # 查找并转换数据文件
        extracted_files = os.listdir(OUTPUT_DIR)
        xlsx_files = [f for f in extracted_files if f.endswith(".xlsx")]
        csv_files = [f for f in extracted_files if f.endswith(".csv")]

        if xlsx_files:
            import pandas as pd
            for fname in xlsx_files:
                xlsx_path = os.path.join(OUTPUT_DIR, fname)
                df = pd.read_excel(xlsx_path)
                df.to_csv(OUTPUT_FILE, index=False)
                logger.info("已转换: %s → %s (%d 行)", fname, OUTPUT_FILE, len(df))
        elif csv_files:
            # 如果是 CSV，直接重命名第一个
            src = os.path.join(OUTPUT_DIR, csv_files[0])
            os.rename(src, OUTPUT_FILE)
            logger.info("已移动: %s → %s", csv_files[0], OUTPUT_FILE)
        else:
            logger.warning("解压后未找到 .xlsx 或 .csv 文件")

        logger.info("数据集就绪！")

    except (OSError, zipfile.BadZipFile, ValueError) as e:
        logger.error("下载失败: %s", e)
        logger.info("请手动下载数据集：")
        logger.info("1. 访问 https://archive.ics.uci.edu/dataset/352/online+retail")
        logger.info("2. 下载 Online Retail.xlsx")
        logger.info("3. 保存为 %s", OUTPUT_FILE)
    finally:
        # 清理残留 zip 文件
        if os.path.exists(zip_path):
            os.remove(zip_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    download_dataset()
