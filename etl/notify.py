"""
多通道告警通知模块 (Notify)
============================
策略模式实现可插拔告警通道。
根据环境变量自动注册可用通道，一个通道失败不影响其他通道。

支持的通道（按需配置，不配不发）：
- 企业微信机器人 Webhook   → WECHAT_WEBHOOK_URL
- 飞书机器人 Webhook        → FEISHU_WEBHOOK_URL
- SMTP 邮件                 → SMTP_HOST + SMTP_PORT + SMTP_USER + SMTP_PASSWORD + SMTP_TO

用法：
    from etl.notify import send_alert
    send_alert("ETL 抽取失败", "详情：连接超时")

Airflow 回调：
    on_failure_callback=lambda ctx: send_alert(
        f"ETL 失败: {ctx['task_instance'].task_id}",
        f"DAG: {ctx['dag'].dag_id}\n"
        f"执行时间: {ctx['execution_date']}\n"
        f"错误: {ctx['exception']}"
    )
"""

import json
import logging
import os
import smtplib
import urllib.request
from email.mime.text import MIMEText
from typing import Callable, List

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ============================================================
# 通道注册表
# ============================================================
_notifiers: List[Callable[[str, str], None]] = []


def register(channel: Callable[[str, str], None]) -> None:
    """注册一个告警通道"""
    _notifiers.append(channel)


def send_alert(title: str, content: str) -> None:
    """
    向所有已注册通道广播告警。

    每个通道独立 try/except，一个通道失败不影响其他通道。
    没有任何注册通道时静默跳过。
    """
    if not _notifiers:
        logger.debug("未配置任何告警通道，跳过发送")
        return

    for notifier in _notifiers:
        try:
            notifier(title, content)
        except Exception:
            logger.exception("告警通道 %s 发送失败", getattr(notifier, "__name__", notifier))


# ============================================================
# 通道实现
# ============================================================

def _send_wecom(title: str, content: str) -> None:
    """企业微信群机器人 — Markdown 格式消息"""
    url = os.getenv("WECHAT_WEBHOOK_URL", "")
    if not url:
        return

    payload = {
        "msgtype": "markdown",
        "markdown": {
            "content": f"## 🚨 {title}\n"
                       f"> {content.replace(chr(10), chr(10) + '> ')}\n\n"
                       f"<font color=\"comment\">订单数仓 ETL · 自动告警</font>"
        },
    }
    _post_json(url, payload)
    logger.info("企业微信告警已发送: %s", title)


def _send_feishu(title: str, content: str) -> None:
    """飞书机器人 — 卡片消息"""
    url = os.getenv("FEISHU_WEBHOOK_URL", "")
    if not url:
        return

    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": f"🚨 {title}"},
                "template": "red",
            },
            "elements": [
                {"tag": "markdown", "content": content},
                {"tag": "hr"},
                {
                    "tag": "note",
                    "elements": [{"tag": "plain_text", "content": "订单数仓 ETL · 自动告警"}],
                },
            ],
        },
    }
    _post_json(url, payload)
    logger.info("飞书告警已发送: %s", title)


def _send_email(title: str, content: str) -> None:
    """SMTP 邮件"""
    smtp_host = os.getenv("SMTP_HOST", "")
    try:
        smtp_port = int(os.getenv("SMTP_PORT", "465"))
    except (ValueError, TypeError):
        smtp_port = 465
    smtp_timeout = int(os.getenv("SMTP_TIMEOUT", "15"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_password = os.getenv("SMTP_PASSWORD", "")
    smtp_to = os.getenv("SMTP_TO", "")

    if not all([smtp_host, smtp_user, smtp_password, smtp_to]):
        return

    msg = MIMEText(content, "plain", "utf-8")
    msg["Subject"] = f"[ETL 告警] {title}"
    msg["From"] = smtp_user
    msg["To"] = smtp_to

    if smtp_port == 465:
        with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=smtp_timeout) as server:
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, smtp_to, msg.as_string())
    else:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=smtp_timeout) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, smtp_to, msg.as_string())

    logger.info("邮件告警已发送: %s → %s", title, smtp_to)


# ============================================================
# 工具函数
# ============================================================

def _post_json(url: str, payload: dict) -> dict:
    """发送 JSON POST 请求，返回解析后的响应"""
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json; charset=utf-8"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        safe_url = url.split("?")[0] if "?" in url else url
        logger.error("Webhook 请求失败: %s — %s", safe_url, e)
        raise


# ============================================================
# 模块加载时自动注册可用通道
# ============================================================

def _auto_register() -> None:
    """根据环境变量自动注册已配置的通知通道"""
    if os.getenv("WECHAT_WEBHOOK_URL"):
        _notifiers.append(_send_wecom)
        logger.info("告警通道已注册: 企业微信")
    if os.getenv("FEISHU_WEBHOOK_URL"):
        _notifiers.append(_send_feishu)
        logger.info("告警通道已注册: 飞书")
    if all(
        os.getenv(k)
        for k in ["SMTP_HOST", "SMTP_USER", "SMTP_PASSWORD", "SMTP_TO"]
    ):
        _notifiers.append(_send_email)
        logger.info("告警通道已注册: SMTP 邮件")


_auto_register()
