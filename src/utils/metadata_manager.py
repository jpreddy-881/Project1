from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.models.metadata_models import PipelineConfig, TableConfig, DQRule
import src.utils.metadata_helper as meta

class MetadataManager:
    """
    Object-Oriented Manager querying metadata Delta tables
    and returning strongly typed Python configuration models.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def get_pipeline(self, pipeline_id: str) -> PipelineConfig:
        """Retrieves strongly typed PipelineConfig from Delta."""
        path = meta.get_table_path("pipeline_config")
        df = self.spark.read.format("delta").load(path).filter(col("pipeline_id") == pipeline_id)
        if df.count() == 0:
            raise ValueError(f"Pipeline ID '{pipeline_id}' not found in configuration.")
            
        row = df.first()
        return PipelineConfig(
            pipeline_id=row["pipeline_id"],
            pipeline_name=row["pipeline_name"],
            description=row["description"],
            schedule_cron=row["schedule_cron"],
            is_active=row["is_active"],
            created_at=row["created_at"]
        )

    def get_table_config(self, pipeline_id: str) -> list[TableConfig]:
        """Retrieves list of strongly typed TableConfig mappings from Delta."""
        path = meta.get_table_path("table_config")
        df = self.spark.read.format("delta").load(path).filter(col("pipeline_id") == pipeline_id)
        
        tables = []
        for row in df.collect():
            tables.append(
                TableConfig(
                    table_id=row["table_id"],
                    pipeline_id=row["pipeline_id"],
                    layer=row["layer"],
                    table_name=row["table_name"],
                    storage_path=row["storage_path"],
                    file_format=row["file_format"],
                    write_mode=row["write_mode"],
                    partition_cols=row["partition_cols"] if row["partition_cols"] else [],
                    merge_keys=row["merge_keys"] if row["merge_keys"] else []
                )
            )
        return tables

    def get_dq_rules(self, pipeline_id: str) -> list[DQRule]:
        """Retrieves list of strongly typed active DQRule configurations from Delta."""
        tables_path = meta.get_table_path("table_config")
        rules_path = meta.get_table_path("dq_rules")
        
        tables_df = self.spark.read.format("delta").load(tables_path).filter(col("pipeline_id") == pipeline_id)
        rules_df = self.spark.read.format("delta").load(rules_path).filter(col("is_active") == True)
        
        joined_df = rules_df.join(tables_df, "table_id")
        
        rules = []
        for row in joined_df.collect():
            rules.append(
                DQRule(
                    rule_id=row["rule_id"],
                    table_id=row["table_id"],
                    column_name=row["column_name"],
                    rule_type=row["rule_type"],
                    expression=row["expression"],
                    action_on_fail=row["action_on_fail"],
                    is_active=row["is_active"]
                )
            )
        return rules
