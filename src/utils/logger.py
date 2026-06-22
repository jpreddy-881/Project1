import os
import sys
from datetime import datetime
from src.utils.config_manager import ConfigManager

class StructuredLogger:
    """
    A structured, reusable logging utility writing formatted entries
    to console and physical log files.
    """
    _instance = None

    def __new__(cls, name: str = "Lakehouse"):
        if cls._instance is None:
            cls._instance = super(StructuredLogger, cls).__new__(cls)
            cls._instance._init_logger(name)
        return cls._instance

    def _init_logger(self, name: str):
        self.name = name
        self.config = ConfigManager()
        self.log_path = self.config.get("log_file", "data/logs/lakehouse.log")
        
        # Ensure target logging directory exists
        log_dir = os.path.dirname(self.log_path)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

    def _log(self, level: str, msg: str):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        formatted = f"{timestamp} [{level}] ({self.name}): {msg}"
        
        # 1. Output to console
        print(formatted)
        
        # 2. Output to log file
        try:
            with open(self.log_path, "a") as f:
                f.write(formatted + "\n")
        except Exception as e:
            print(f"FAILED TO WRITE LOG TO FILE: {str(e)}", file=sys.stderr)

    def info(self, msg: str):
        self._log("INFO", msg)

    def warn(self, msg: str):
        self._log("WARN", msg)

    def error(self, msg: str):
        self._log("ERROR", msg)
