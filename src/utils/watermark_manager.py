from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession
import src.utils.metadata_helper as meta

class WatermarkManager:
    """
    Manages watermark configuration and state for multiple pipelines.
    Reuses Delta metadata helper functions to read and write watermark settings.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def get_watermark(self, pipeline_id: str) -> Optional[datetime]:
        """Retrieves the last successful watermark timestamp for the given pipeline."""
        wtm_cfg = meta.get_watermark_config(self.spark, pipeline_id)
        if not wtm_cfg:
            return None
        return wtm_cfg.get("last_watermark_value")

    def get_watermark_config(self, pipeline_id: str) -> Optional[dict]:
        """Retrieves the full watermark configuration dictionary for the pipeline."""
        return meta.get_watermark_config(self.spark, pipeline_id)

    def update_watermark(self, pipeline_id: str, new_watermark: datetime) -> None:
        """Updates the watermark value for the given pipeline in the Delta control table."""
        meta.update_watermark_value(self.spark, pipeline_id, new_watermark)
