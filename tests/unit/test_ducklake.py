import pytest

from millpond.ducklake import _escape_libpq, _sanitize_setting_value


class TestSanitizeSettingValue:
    def test_plain_value(self):
        assert _sanitize_setting_value("us-east-1") == "us-east-1"

    def test_sql_injection_rejected(self):
        with pytest.raises(ValueError, match="Illegal character"):
            _sanitize_setting_value("us-east-1'; DROP TABLE x; --")

    def test_single_quote_rejected(self):
        with pytest.raises(ValueError, match="Illegal character"):
            _sanitize_setting_value("it's")

    def test_normal_s3_values(self):
        assert _sanitize_setting_value("minioadmin") == "minioadmin"
        assert _sanitize_setting_value("false") == "false"
        assert _sanitize_setting_value("path") == "path"
        assert _sanitize_setting_value("minio:9000") == "minio:9000"

    def test_url_style_values(self):
        assert _sanitize_setting_value("s3.amazonaws.com") == "s3.amazonaws.com"

    def test_access_key_with_slashes(self):
        assert _sanitize_setting_value("ABC/def123+key") == "ABC/def123+key"

    def test_base64_padding(self):
        assert _sanitize_setting_value("abc123==") == "abc123=="

    def test_empty_rejected(self):
        with pytest.raises(ValueError, match="Illegal character"):
            _sanitize_setting_value("")


class TestEscapeLibpq:
    def test_plain_value(self):
        assert _escape_libpq("ducklake") == "'ducklake'"

    def test_single_quote(self):
        assert _escape_libpq("pass'word") == "'pass\\'word'"

    def test_backslash(self):
        assert _escape_libpq("pass\\word") == "'pass\\\\word'"

    def test_both(self):
        assert _escape_libpq("it's\\complex") == "'it\\'s\\\\complex'"

    def test_none(self):
        assert _escape_libpq(None) == "''"
