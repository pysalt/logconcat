import configparser
import logging
import os
import re
from typing import List

CONFIG_PATH = 'config.ini'
DEFAULT_LOGS_PATH = '/var/log/ofelia/logs/'
DEFAULT_SAVE_PATH = '/var/log/ofelia/'


class ConfigNotFoundError(Exception):
    pass


class LogConcat:

    required_fields = ['stdout_pattern', 'stderr_pattern']

    def __init__(self):
        self._stdout_pattern = None
        self._stderr_pattern = None
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
        except KeyError:
            raise ValueError(f'{self.required_fields} field must be in {CONFIG_PATH} section main')

        if config.has_section('extra'):
            for key in config['extra']:
                self.extra[key] = config['extra'][key]

    def _get_files_list_by_pattern(self, pattern):
        files = []
        root_path = self.extra.get('logs_path') or DEFAULT_LOGS_PATH
        for f in os.listdir(root_path):
            f_path = os.path.join(root_path, f)
            if os.path.isfile(f_path) and re.match(pattern, f):
                files.append(f)

        return self._sort_files_by_modification_time(files)

    @staticmethod
    def _sort_files_by_modification_time(files: List[str]):
        return sorted(files, key=lambda f: os.path.getmtime(f))

    def _merge_log_files(self):
        pass


def main():
    try:
        concater = LogConcat()
    except Exception as e:
        logging.error(str(e))


if __name__ == "__main__":
    main()
