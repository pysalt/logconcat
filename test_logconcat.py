import os

import pytest
import time
from unittest.mock import patch, mock_open

from logconcat import LogConcat

STDOUT_BASE_NAME = 'stdout_log.log'
STDERR_BASE_NAME = 'stderr_log.log'
STDOUT_PATTERN = r'.*stdout_log\.log'
STDERR_PATTERN = r'.*stderr_log\.log'

TEST_CONFIG = """
[main]
stdout_pattern = {}
stderr_pattern = {}
chunk = 10

[extra]
logs_path = {}
save_path = {}
stdout_log_name = {}
"""


@pytest.fixture
def input_dir(tmpdir_factory):
    return tmpdir_factory.mktemp('input')


@pytest.fixture
def output_dir(tmpdir_factory):
    return tmpdir_factory.mktemp('output')


@pytest.fixture
def log_files(input_dir):
    files = {}
    for i in range(1, 4):
        name = str(i) + STDOUT_BASE_NAME
        f = input_dir.join(name)
        data = str(i) * 20
        files[str(f)] = data
        f.write(data)
        time.sleep(0.0001)

    return files


@pytest.fixture
def patched_config(tmpdir_factory, input_dir, log_files, output_dir):
    data = TEST_CONFIG.format(STDOUT_PATTERN, STDERR_PATTERN, input_dir, output_dir, STDOUT_BASE_NAME)
    f = output_dir.join('config.ini')
    f.write(data)

    return f


class TestLogConcat:
    def test_merge_stdout_logs__success(self, log_files, output_dir, patched_config):
        with patch('logconcat.CONFIG_PATH', patched_config):
            con = LogConcat()
        con.merge_stdout_logs()

        res = output_dir.join(STDOUT_BASE_NAME).read()
        for path, data in log_files.items():
            assert not os.path.exists(path)
            assert data in res
