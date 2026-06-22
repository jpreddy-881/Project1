import os
import sys
import threading
from pyspark.sql import SparkSession
from src.utils.config_manager import ConfigManager

class SparkSessionManager:
    """
    Thread-safe Singleton Manager for the PySpark Session.
    Exposes a unified, configuration-driven connection to Delta Lake.
    """
    _instance = None
    _spark = None
    _lock = threading.Lock()

    def __new__(cls, app_name: str = "LakehousePlatform"):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(SparkSessionManager, cls).__new__(cls)
                cls._instance._init_spark(app_name)
            return cls._instance

    def _init_spark(self, app_name: str):
        # Ensure PySpark workers use the correct Python executable
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        
        # Load environment-specific properties from ConfigManager
        config = ConfigManager()
        
        warehouse_dir = os.path.abspath(config.get("warehouse_dir", "../../data/warehouse")).replace("\\", "/")
        derby_home = os.path.abspath(config.get("derby_home", "../../data/metadata/.derby")).replace("\\", "/")
        spark_packages = config.get("spark_packages", "io.delta:delta-spark_4.1_2.13:4.1.0")

        self._spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.warehouse.dir", warehouse_dir) \
            .config("spark.jars.packages", spark_packages) \
            .config("spark.sql.sources.default", "delta") \
            .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby_home}") \
            .getOrCreate()

    @property
    def spark(self) -> SparkSession:
        """Returns the active SparkSession instance."""
        return self._spark

def get_spark_session(app_name: str = "LakehousePlatform") -> SparkSession:
    """Helper method providing backwards-compatibility for existing orchestrations."""
    return SparkSessionManager(app_name).spark
