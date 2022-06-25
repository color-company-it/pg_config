from pg_config.readers import load_yaml, load_json


def _handle_configuration(configuration: [str, dict]) -> dict:
    """
    Assess if the configuration is a config object
    or a path to a config file.
    :return: Dict object.
    """
    # if dict, just return, else load file
    if isinstance(configuration, dict):
        return configuration
    if isinstance(configuration, str):
        file_type = configuration.split(".")[-1]
        if file_type in ["yaml", "yml"]:
            return load_yaml(filepath=configuration)
        elif file_type == "json":
            return load_json(filepath=configuration)
        else:
            raise EnvironmentError(
                f"Provided configuration filepath {file_type}"
                f"is unsupported, use json or yaml."
            )
