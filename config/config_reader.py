import configparser


def get_config_value(config_file_path: str, section: str, option: str) -> str:
    config = configparser.ConfigParser()
    config.read(config_file_path)

    return config.get(section, option)

