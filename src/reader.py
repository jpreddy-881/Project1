from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

def read_source(
    spark: SparkSession,
    source_path: str,
    source_format: str,
    watermark_start: datetime = None,
    watermark_end: datetime = None,
    watermark_col: str = "last_updated_at"
) -> DataFrame:
    """
    Reads data from the source path, applying incremental filtering based on watermark boundaries.
    """
    df = spark.read.format(source_format).load(source_path)
    
    # Apply watermark filters if column is present in schema
    if watermark_col in df.columns:
        if watermark_start:
            df = df.filter(col(watermark_col) > watermark_start)
        if watermark_end:
            df = df.filter(col(watermark_col) <= watermark_end)
            
    return df
