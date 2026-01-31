import yaml as py

def load_yaml(file_path: str) -> dict:
    """
    Load a YAML configuration file and return its contents as a dictionary.

    :param file_path: Path to the YAML file.
    :type file_path: str
    :return: Dictionary containing the YAML file contents.
    :rtype: dict
    """
    try:
        with open(file_path, 'r') as stream:
            config = py.load(stream, Loader=py.FullLoader)
        return config
    except Exception as e:
        raise RuntimeError(f"Error loading YAML file {file_path}: {e}")