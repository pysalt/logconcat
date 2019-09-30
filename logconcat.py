import configparser
import logging
import os
import re
from typing import List

CONFIG_PATH = 'config.ini'
DEFAULTS = {
    'logs_path': '/var/log/ofelia/logs/',
    'save_path': '/var/log/ofelia/',
}


class ConfigNotFoundError(Exception):
    pass


class LogConcat:

    required_fields = ['stdout_pattern', 'stderr_pattern', 'chunk']

    def __init__(self):
        self._stdout_pattern = None
        self._stderr_pattern = None
        self.chunk_size = None
        self.extra = {}

        self._parse_config()

    @property
    def stdout_pattern(self):
        return self._stdout_pattern

    @stdout_pattern.setter
    def stdout_pattern(self, new_pattern: str):
        self._stdout_pattern = re.compile(new_pattern, re.IGNORECASE)

    @property
    def stderr_pattern(self):
        return self._stderr_pattern

    @stderr_pattern.setter
    def stderr_pattern(self, new_pattern: str):
        self._stderr_pattern = re.compile(new_pattern, re.IGNORECASE)

    def _parse_config(self):
        config = configparser.ConfigParser()
        if not os.path.exists(CONFIG_PATH):
            raise ConfigNotFoundError(f'Not found {CONFIG_PATH}.')

        config.read_file(open(CONFIG_PATH))
        try:
            self.stdout_pattern = config['main']['stdout_pattern']
            self.stderr_pattern = config['main']['stderr_pattern']
            self.chunk_size = config.getint('main', 'chunk')
        except KeyError:
            raise ValueError(f'{self.required_fields} field must be in {CONFIG_PATH} section main')

        for key in DEFAULTS:
            self.extra[key] = config.get('extra', key, fallback=None) or DEFAULTS[key]

    def _get_files_list_by_pattern(self, pattern):
        files = []

        for f in os.listdir(self.extra['logs_path']):
            f_path = os.path.join(self.extra['logs_path'], f)
            if os.path.isfile(f_path) and re.match(pattern, f):
                files.append(f)

        return self._sort_files_by_modification_time(files)

    @staticmethod
    def _sort_files_by_modification_time(files: List[str]):
        return sorted(files, key=lambda f: os.path.getmtime(f))

    def _merge_log_files(self, pattern, log_name: str):
        if not os.path.exists(self.extra['save_path']):
            os.mkdir(self.extra['save_path'])
        files = self._get_files_list_by_pattern(pattern)

        with open(self.extra['save_path'], 'a+b') as f_out:
            for batch in self._chunks(files, self.chunk_size):
                f_out.write(self._read_batch_of_files(batch))

    def _read_batch_of_files(self, files_batch: List[str]) -> bytes:
        buf = b''
        for name in files_batch:
            with open(os.path.join(self.extra['logs_path'], name), 'rb') as f:
                buf += f.read()
        return buf

    def _remove_batch_of_files(self, files_batch: List[str]):
        pass

    @staticmethod
    def _chunks(r: List, size: int):
        """
        Splits an iterable into chunks of size.
        """
        offset = 0
        while 1:
            res = r[offset: offset + size]
            if not res:
                raise StopIteration
            yield res
            offset += size


def main():
    try:
        concater = LogConcat()
    except Exception as e:
        logging.error(str(e))


if __name__ == "__main__":
    main()
