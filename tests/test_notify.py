"""
notify 模块单元测试

覆盖：通道注册、多通道广播、单一通道失败不影响其他通道。
"""

import os
import pytest
from etl.notify import send_alert, _notifiers, register


class TestSendAlertNoChannels:
    """无通道配置时不报错"""

    def test_noop_when_empty(self, monkeypatch):
        # 确保 _notifiers 为空
        monkeypatch.setattr("etl.notify._notifiers", [])
        # 不应抛异常
        send_alert("测试标题", "测试内容")


class TestMultiChannelBroadcast:
    """多通道广播，一个失败不影响其他"""

    def test_one_failure_doesnt_block_others(self, monkeypatch):
        called = []

        def good(title, content):
            called.append(("good", title))

        def bad(title, content):
            called.append(("bad", title))
            raise RuntimeError("模拟通道异常")

        monkeypatch.setattr("etl.notify._notifiers", [good, bad])
        send_alert("报警标题", "报警内容")

        # good 通道必须被调用，bad 通道虽然抛异常但也应被调用
        assert ("good", "报警标题") in called
        assert ("bad", "报警标题") in called

    def test_all_channels_called(self, monkeypatch):
        results = []

        def chan_a(title, content):
            results.append("A")

        def chan_b(title, content):
            results.append("B")

        monkeypatch.setattr("etl.notify._notifiers", [chan_a, chan_b])
        send_alert("x", "y")

        assert results == ["A", "B"]


class TestAutoRegister:
    """环境变量驱动自动注册"""

    def test_wecom_registered_when_url_set(self, monkeypatch):
        monkeypatch.setenv("WECHAT_WEBHOOK_URL", "https://example.com/webhook")
        monkeypatch.setattr("etl.notify._notifiers", [])

        from etl.notify import _auto_register

        _auto_register()

        from etl.notify import _notifiers as n

        # 企业微信通道已注册
        assert any("wecom" in fn.__name__ for fn in n)
        # 飞书通道不应注册（未设 URL）
        assert not any("feishu" in fn.__name__ for fn in n)

    def test_no_registration_without_env(self, monkeypatch):
        monkeypatch.delenv("WECHAT_WEBHOOK_URL", raising=False)
        monkeypatch.delenv("FEISHU_WEBHOOK_URL", raising=False)
        monkeypatch.delenv("SMTP_HOST", raising=False)
        monkeypatch.setattr("etl.notify._notifiers", [])

        from etl.notify import _auto_register

        _auto_register()

        from etl.notify import _notifiers as n

        assert len(n) == 0
