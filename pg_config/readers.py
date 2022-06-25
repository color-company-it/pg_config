import json

import yaml


def load_yaml(filepath: str) -> dict:
    """
    Loads yaml file to python object.
    :param filepath: Path to yaml file.
    :return: Python object.
    """
    with open(filepath, "r") as _file:
        return yaml.safe_load(filepath)


def load_json(filepath: str) -> dict:
    """
    Loads json file to python object.
    :param filepath: Path to json file.
    :return: Python object.
    """
    with open(filepath, "r") as _file:
        return json.loads(filepath)
