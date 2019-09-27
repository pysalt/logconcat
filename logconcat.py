import configparser
import logging
import os

CONFIG_PATH = 'config.ini'


class ConfigNotFoundError(Exception):
    pass


class LogConcat:

    required_fields = ['stdout_pattern', 'stderr_pattern', 'time_mask']

    def __init__(self):
        self.stdout_pattern = None
        self.stderr_pattern = None
        self.time_mask = None
        self.extra = {}

        self._parse_config()

    def _parse_config(self):
        config = configparser.ConfigParser()
        if not os.path.exists(CONFIG_PATH):
            raise ConfigNotFoundError(f'Not found {CONFIG_PATH}.')

        config.read_file(open(CONFIG_PATH))
        try:
            for attr in self.required_fields:
                setattr(self, attr, config['main'][attr])
        except KeyError:
            raise ValueError(f'{self.required_fields} field must be in {CONFIG_PATH} section main')

        if config.has_section('extra'):
            for key in config['extra']:
                self.extra[key] = config['extra'][key]


def main():
    try:
        concater = LogConcat()
    except Exception as e:
        logging.error(e)


if __name__ == "__main__":
    main()
