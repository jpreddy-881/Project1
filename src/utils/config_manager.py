import os

class ConfigManager:
    """
    Manages loading configuration files dynamically by parsing yaml
    profiles (dev, test, prod) without external dependencies.
    """
    _instance = None
    _config = {}
    _env = "dev"

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        # Resolve active environment profile (dev, test, prod)
        self._env = os.getenv("ENV", "dev").lower()
        if self._env not in ("dev", "test", "prod"):
            self._env = "dev"
            
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../config/config.yaml"))
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found at: {config_path}")
            
        with open(config_path, "r") as f:
            content = f.read()
            
        self._config = self._parse_yaml(content)

    def _parse_yaml(self, content: str) -> dict:
        """Lightweight pure-python YAML parser for environment profiles."""
        result = {}
        current_profile = None
        
        for line in content.splitlines():
            line = line.split('#', 1)[0].strip() # strip comments
            if not line:
                continue
                
            if line.endswith(':'):
                current_profile = line[:-1].strip()
                result[current_profile] = {}
            elif ':' in line:
                parts = line.split(':', 1)
                k = parts[0].strip()
                v = parts[1].strip().strip('"\'')
                
                if current_profile:
                    result[current_profile][k] = v
                else:
                    result[k] = v
        return result

    def get(self, key: str, default=None):
        """Retrieves a configuration parameter for the active environment profile."""
        profile_config = self._config.get(self._env, {})
        return profile_config.get(key, default)

    @property
    def env(self) -> str:
        """Returns the active environment profile."""
        return self._env
