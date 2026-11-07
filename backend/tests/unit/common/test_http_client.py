"""Proxy parsing must not disclose credentials in errors or logs."""

import pytest

from infrastructure.services.common.http_client import format_proxy_url, load_proxies


def test_invalid_proxy_error_does_not_include_input():
    secret = "private-password"
    with pytest.raises(ValueError) as exc:
        format_proxy_url(f"user:{secret}@invalid@host")
    assert secret not in str(exc.value)


def test_invalid_proxy_is_skipped_without_logging_credentials(tmp_path, caplog):
    secret = "private-password"
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text(f"user:{secret}@invalid@host\n")
    assert load_proxies(str(proxy_file)) == []
    assert secret not in caplog.text
