import configparser
import logging
import os
import re
from typing import List

CONFIG_PATH = 'config.ini'
DEFAULTS = {
    'logs_path': '/var/log/ofelia/logs/',
    'save_path': '/var/log/ofelia/',
    'stdout_log_name': 'stdout.log',
    'stderr_log_name': 'stderr.log',
}


class ConfigNotFoundError(Exception):
    pass


class LogConcat:

    required_fields = ['stdout_pattern', 'stderr_pattern', 'chunk']

    def __init__(self):
        self._stdout_pattern = None
        self._stderr_pattern = None
        self.chunk_size = None
        self.files_to_remove = []
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

    def merge_stdout_logs(self):
        self._merge_log_files(self.stdout_pattern, self.extra['stdout_log_name'])

    def merge_stderr_logs(self):
        self._merge_log_files(self.stderr_pattern, self.extra['stderr_log_name'])

    def _parse_config(self):
        config = configparser.ConfigParser()
        if not os.path.exists(CONFIG_PATH):
            raise ConfigNotFoundError(f'Not found {CONFIG_PATH}.')

        with open(CONFIG_PATH) as f:
            config.read_file(f)
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
                files.append(f_path)

        return self._sort_files_by_modification_time(files)

    @staticmethod
    def _sort_files_by_modification_time(files: List[str]):
        return sorted(files, key=lambda f: os.path.getmtime(f))

    def _merge_log_files(self, pattern, log_name: str):
        if not os.path.exists(self.extra['save_path']):
            os.mkdir(self.extra['save_path'])
        files = self._get_files_list_by_pattern(pattern)
        main_log_path = os.path.join(self.extra['save_path'], log_name)
        logging.info(f'Start moving {len(files)} files to {main_log_path}.')

        with open(main_log_path, 'a+b') as f_out:
            counter = 0
            for batch in self._chunks(files, self.chunk_size):
                f_out.write(self._read_batch_of_files(batch))
                self._remove_batch_of_files()
                counter += self.chunk_size
                logging.info(f'Processed {counter} files.')

    def _read_batch_of_files(self, files_batch: List[str]) -> bytes:
        buf = b''
        for path in files_batch:
            with open(path, 'rb') as f:
                buf += f.read()
            self.files_to_remove.append(path)
        return buf

    def _remove_batch_of_files(self):
        if self.files_to_remove:
            for path in self.files_to_remove:
                os.remove(path)
            logging.info(f'Removed {len(self.files_to_remove)} files.')
        logging.info('Not files to remove')

    @staticmethod
    def _chunks(r: List, size: int):
        """
        Splits an iterable into chunks of size.
        """
        offset = 0
        while 1:
            res = r[offset: offset + size]
            if not res:
                return
            yield res
            offset += size


def main():
    try:
        con = LogConcat()
        con.merge_stderr_logs()
        con.merge_stdout_logs()
    except Exception as e:
        logging.error(str(e))


if __name__ == "__main__":
    main()
