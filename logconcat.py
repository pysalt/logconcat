import configparser
import logging
import os
import re
from typing import List, Optional
from datetime import datetime

CONFIG_NAME = 'config.ini'
DEFAULTS = {
    'logs_path': '/var/log/ofelia/logs/',
    'save_path': '/var/log/ofelia/',
    'stdout_log_name': 'stdout.log',
    'stderr_log_name': 'stderr.log',
    'time_mask': '%Y%m%d_%H%M%S',
}
DEFAULT_SCHEDULER_PATTER = r'\.json$'


def setup_logger(logger):
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d_%H:%M:%S')
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(ch)


app_logger = logging.getLogger(__name__)
setup_logger(app_logger)


class ConfigNotFoundError(Exception):
    pass


class TimeMask:
    default_delimiter: str = '_'

    def __init__(self, mask):
        if mask is None:
            raise ValueError('Time mask must be specified.')
        self._value = mask
        self._delimiter_count = self.value.count(self.default_delimiter)

    @property
    def value(self) -> str:
        return self._value

    @property
    def delimiter_count(self) -> int:
        return self._delimiter_count


class LogConcat:

    required_fields = ['stdout_pattern', 'stderr_pattern', 'chunk']

    def __init__(self):
        self._stdout_pattern = None
        self._stderr_pattern = None
        self._scheduler_patter = None
        self.chunk_size = None
        self.files_to_remove = []
        self.sort_by_time_mask = False
        self.time_mask: Optional[TimeMask] = None
        self.extra = {}

        self._parse_config()
        self._configure()

    @property
    def stdout_pattern(self):
        return self._stdout_pattern

    @stdout_pattern.setter
    def stdout_pattern(self, new_pattern: str):
        self._stdout_pattern = self._compile_pattern(new_pattern)

    @property
    def stderr_pattern(self):
        return self._stderr_pattern

    @stderr_pattern.setter
    def stderr_pattern(self, new_pattern: str):
        self._stderr_pattern = self._compile_pattern(new_pattern)

    @property
    def scheduler_patter(self):
        return self._scheduler_patter

    @scheduler_patter.setter
    def scheduler_patter(self,  new_pattern: str):
        self._scheduler_patter = self._compile_pattern(new_pattern)

    @staticmethod
    def _compile_pattern(pattern):
        return re.compile(pattern, re.IGNORECASE)

    def merge_stdout_logs(self):
        self._merge_log_files(self.stdout_pattern, self.extra['stdout_log_name'])

    def merge_stderr_logs(self):
        self._merge_log_files(self.stderr_pattern, self.extra['stderr_log_name'])

    def remove_scheduler_logs(self):
        files = self._get_files_list_by_pattern(self.scheduler_patter)
        for path in files:
            os.remove(path)
        app_logger.info(f'Removed {len(files)} files of the schedulers logs')

    def _parse_config(self):
        config = configparser.ConfigParser()
        config_path = self._get_config_path()

        config.read(config_path)
        try:
            self.stdout_pattern = config['main']['stdout_pattern']
            self.stderr_pattern = config['main']['stderr_pattern']
            self.chunk_size = config.getint('main', 'chunk')
        except KeyError:
            raise ValueError(f'{self.required_fields} field must be in {CONFIG_NAME} section main')
        self.sort_by_time_mask = config.getboolean('main', 'sort_by_time_mask', fallback=False)
        self.scheduler_patter = config.get('main', 'scheduler_patter', fallback=DEFAULT_SCHEDULER_PATTER)

        for key in DEFAULTS:
            self.extra[key] = config.get('extra', key, fallback=None) or DEFAULTS[key]

    @staticmethod
    def _get_config_path() -> str:
        """
        in the current version, the code expects that the config is next to the script
        :return:
        """
        base = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(base, CONFIG_NAME)
        if not os.path.exists(path):
            raise ConfigNotFoundError(f'Not found {CONFIG_NAME} in path={base}.')

        return path

    def _configure(self):
        if self.sort_by_time_mask:
            self.time_mask = TimeMask(self.extra.pop('time_mask', None))

    def _get_files_list_by_pattern(self, pattern):
        files = []

        for f in os.listdir(self.extra['logs_path']):
            f_path = os.path.join(self.extra['logs_path'], f)
            if os.path.isfile(f_path) and re.search(pattern, f):
                files.append(f_path)

        return files

    def _get_files_list_by_pattern_sorted(self, pattern):
        files = self._get_files_list_by_pattern(pattern)
        return self._sort_files_by_modification_time(files)

    def _sort_files_by_modification_time(self, files: List[str]):
        return sorted(files, key=self._sort)

    def _sort(self, value: str):
        if self.sort_by_time_mask:
            return self._get_time_mask_sorting_key(value)
        else:
            return os.path.getmtime(value)

    def _get_time_mask_sorting_key(self, value: str):
        file_name = os.path.basename(value)
        split_count = self.time_mask.delimiter_count + 1
        time_part = file_name.split(TimeMask.default_delimiter, maxsplit=split_count)[:-1]
        time_part_row = TimeMask.default_delimiter.join(time_part)
        creation_time = datetime.strptime(time_part_row, self.time_mask.value)
        return creation_time.timestamp()

    def _merge_log_files(self, pattern, log_name: str):
        if not os.path.exists(self.extra['save_path']):
            os.mkdir(self.extra['save_path'])
        files = self._get_files_list_by_pattern_sorted(pattern)
        main_log_path = os.path.join(self.extra['save_path'], log_name)
        app_logger.info(f'Start moving {len(files)} files to {main_log_path}.')

        with open(main_log_path, 'a+b') as f_out:
            counter = 0
            for batch in self._chunks(files, self.chunk_size):
                f_out.write(self._read_batch_of_files(batch))
                self._remove_batch_of_files()
                counter += self.chunk_size
                app_logger.info(f'Processed {counter} files.')

    def _read_batch_of_files(self, files_batch: List[str]) -> bytes:
        buf = b''
        for path in files_batch:
            with open(path, 'rb') as f:
                buf += f.read()
            self.files_to_remove.append(path)
        return buf

    def _remove_batch_of_files(self):
        if not self.files_to_remove:
            app_logger.info('Not files to remove')
            return

        app_logger.info(f'Start removing {len(self.files_to_remove)} files.')
        while self.files_to_remove:
            os.remove(self.files_to_remove.pop())
        app_logger.info(f'All files removed.')

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
        app_logger.info('Start processing logs.')
        con = LogConcat()
        con.merge_stderr_logs()
        con.merge_stdout_logs()
        con.remove_scheduler_logs()
        app_logger.info('End task.')
    except Exception as e:
        app_logger.exception(e)


if __name__ == "__main__":
    main()
