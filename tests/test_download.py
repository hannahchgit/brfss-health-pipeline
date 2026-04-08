"""Unit tests for ingestion/download_brfss.py"""

import hashlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ingestion.download_brfss import compute_sha256, xpt_path, sha256_path


def test_xpt_path():
    result = xpt_path(2022, Path("/data/raw"))
    assert result == Path("/data/raw/LLCP2022.XPT")


def test_sha256_path():
    result = sha256_path(2022, Path("/data/raw"))
    assert result == Path("/data/raw/LLCP2022.XPT.sha256")


def test_compute_sha256(tmp_path):
    test_file = tmp_path / "test.txt"
    content = b"hello BRFSS"
    test_file.write_bytes(content)

    expected = hashlib.sha256(content).hexdigest()
    actual = compute_sha256(test_file)
    assert actual == expected


def test_xpt_already_exists_skips_download(tmp_path):
    """If XPT file exists and force=False, download_year should return early."""
    from ingestion.download_brfss import download_year

    year = 2022
    xpt = xpt_path(year, tmp_path)
    xpt.write_bytes(b"fake xpt content " * 1_000_000)  # > MIN_XPT_BYTES

    with patch("ingestion.download_brfss.requests.get") as mock_get:
        result = download_year(year, tmp_path, force=False)
        mock_get.assert_not_called()
        assert result == xpt
