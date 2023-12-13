import configparser


def get_config_value(config_file_path: str, section: str, option: str) -> str:
    """ retrieves a configuration value from a specified section and option in a config file """

    config = configparser.ConfigParser()
    config.read(config_file_path)

    return config.get(section, option)

