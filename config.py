import tomllib

def load_config(path: str = "config.toml") -> dict:
    with open(path, 'rb') as f:
        return tomllib.load(f)
