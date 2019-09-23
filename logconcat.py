from exceptions import ConfigNotFoundError
import configparser
import os

CONFIG_NAME = 'config.ini'


class LogConcat:

    required_fields = ['stdout_pattern', 'stderr_pattern', 'rotate', 'time_mask']

    def __init__(self):
        self.stdout_pattern = None
        self.stderr_pattern = None
        self.rotate = None
        self.time_mask = None

    def _parse_config(self):
        config = configparser.ConfigParser()
        if not os.path.exists(CONFIG_NAME):
            raise ConfigNotFoundError(f'Not found {CONFIG_NAME}.')

        config.read_file(open(CONFIG_NAME))
        try:
            for attr in self.required_fields:
                setattr(self, attr, config[attr])
        except KeyError:
            raise ValueError(f'{self.required_fields} field must be in config.ini')


def main():
    pass


if __name__ == "__main__":
    main()
