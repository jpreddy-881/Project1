from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name, lit, col
from src.models.metadata_models import TableConfig
from src.models.pipeline_context import PipelineContext

class IngestionEngine:
    """
    Generic Bronze Ingestion Engine.
    
    Loads source files (CSV, Parquet, JSON) dynamically based on metadata parameters, 
    applies incremental watermark filters, and automatically injects audit columns.
    
    ### Audit Requirements Explanation:
    In production-grade Lakehouses, audit columns are critical for data lineage and tracking:
    1. ingestion_timestamp: Records the exact time Spark loaded the row. Enables resolving
       late arriving records and finding processing latency.
    2. source_file_name: Records the physical storage file path containing the source record.
       Essential for tracing malformed row source files back to upstream applications.
    3. batch_id: Links each record directly to the PipelineContext execution run_id, 
       permitting end-to-end reconciliation and failure debug tracking.
    """
    def __init__(self, spark: SparkSession, context: PipelineContext):
        self.spark = spark
        self.context = context

    def ingest(
        self,
        source_config: TableConfig,
        watermark_col: Optional[str] = None,
        watermark_start: Optional[datetime] = None,
        watermark_end: Optional[datetime] = None
    ) -> DataFrame:
        """
        Ingests data from the source path using configurations defined in metadata.
        Support: JSON, CSV, Parquet.
        
        Behavior (Full Load vs Incremental Load) is driven by metadata:
        - Incremental: Triggered when watermark_col is present in both configurations and schema.
        - Full Load: Triggered otherwise (no watermark filters are applied).
        """
        fmt = source_config.file_format.lower()
        if fmt not in ("csv", "parquet", "json"):
            raise ValueError(f"Unsupported file format: {source_config.file_format}")

        # Load DataFrame dynamically based on format configuration
        df = self.spark.read.format(fmt).load(source_config.storage_path)

        # Apply watermark filter if configured and present in dataset schema
        if watermark_col and watermark_col in df.columns:
            if watermark_start:
                df = df.filter(col(watermark_col) > watermark_start)
            if watermark_end:
                df = df.filter(col(watermark_col) <= watermark_end)

        # Inject audit metadata columns
        df = df.withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("source_file_name", input_file_name()) \
               .withColumn("batch_id", lit(self.context.run_id))

        return df
