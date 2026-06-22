import os
import json
from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, BooleanType, ArrayType
from pyspark.sql.functions import col, desc

# Default paths mapping under metadata root directory
METADATA_TABLES = {
    "pipeline_config": "pipeline_config",
    "table_config": "table_config",
    "column_config": "column_config",
    "dq_rules": "dq_rules",
    "pipeline_dependencies": "pipeline_dependencies",
    "watermark_control": "watermark_control",
    "audit_pipeline_execution": "audit_pipeline_execution",
    "audit_pipeline_steps": "audit_pipeline_steps",
    "audit_data_reconciliation": "audit_data_reconciliation",
    "data_quality_log": "data_quality_log"
}

# Config schemas
PIPELINE_CONFIG_SCHEMA = StructType([
    StructField("pipeline_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("description", StringType(), True),
    StructField("schedule_cron", StringType(), True),
    StructField("is_active", BooleanType(), False),
    StructField("created_at", TimestampType(), False)
])

TABLE_CONFIG_SCHEMA = StructType([
    StructField("table_id", StringType(), False),
    StructField("pipeline_id", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("storage_path", StringType(), False),
    StructField("file_format", StringType(), False),
    StructField("write_mode", StringType(), False),
    StructField("partition_cols", ArrayType(StringType()), True),
    StructField("merge_keys", ArrayType(StringType()), True)
])

COLUMN_CONFIG_SCHEMA = StructType([
    StructField("column_id", StringType(), False),
    StructField("table_id", StringType(), False),
    StructField("column_name", StringType(), False),
    StructField("data_type", StringType(), False),
    StructField("is_nullable", BooleanType(), False),
    StructField("is_primary_key", BooleanType(), False),
    StructField("description", StringType(), True)
])

DQ_RULES_SCHEMA = StructType([
    StructField("rule_id", StringType(), False),
    StructField("table_id", StringType(), False),
    StructField("column_name", StringType(), False),
    StructField("rule_type", StringType(), False),
    StructField("expression", StringType(), False),
    StructField("action_on_fail", StringType(), False),
    StructField("is_active", BooleanType(), False)
])

PIPELINE_DEPENDENCIES_SCHEMA = StructType([
    StructField("dependency_id", StringType(), False),
    StructField("pipeline_id", StringType(), False),
    StructField("parent_pipeline_id", StringType(), False),
    StructField("is_active", BooleanType(), False)
])

# 6. watermark_control Schema
WATERMARK_CONTROL_SCHEMA = StructType([
    StructField("watermark_id", StringType(), False),
    StructField("pipeline_id", StringType(), False),
    StructField("table_id", StringType(), False),
    StructField("watermark_column", StringType(), False),
    StructField("last_watermark_value", TimestampType(), True),
    StructField("lookback_minutes", LongType(), False),
    StructField("backfill_status", StringType(), False),
    StructField("backfill_start", TimestampType(), True),
    StructField("backfill_end", TimestampType(), True),
    StructField("updated_at", TimestampType(), False)
])

# Operational audit schemas
AUDIT_PIPELINE_EXECUTION_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("pipeline_id", StringType(), False),
    StructField("execution_start", TimestampType(), False),
    StructField("execution_end", TimestampType(), True),
    StructField("watermark_start", TimestampType(), True),
    StructField("watermark_end", TimestampType(), True),
    StructField("records_read", LongType(), True),
    StructField("records_written", LongType(), True),
    StructField("status", StringType(), False),
    StructField("error_message", StringType(), True)
])

AUDIT_PIPELINE_STEPS_SCHEMA = StructType([
    StructField("step_run_id", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("step_name", StringType(), False),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), False),
    StructField("status", StringType(), False),
    StructField("records_processed", LongType(), True),
    StructField("error_details", StringType(), True)
])

AUDIT_DATA_RECONCILIATION_SCHEMA = StructType([
    StructField("reconciliation_id", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("pipeline_id", StringType(), False),
    StructField("source_count", LongType(), False),
    StructField("target_inserted_count", LongType(), False),
    StructField("target_updated_count", LongType(), False),
    StructField("quarantine_count", LongType(), False),
    StructField("discrepancy_count", LongType(), False),
    StructField("reconciliation_status", StringType(), False),
    StructField("notes", StringType(), True)
])

DATA_QUALITY_LOG_SCHEMA = StructType([
    StructField("log_id", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("rule_id", StringType(), False),
    StructField("failed_records", LongType(), False),
    StructField("quarantine_path", StringType(), True),
    StructField("status", StringType(), False)
])

def get_metadata_root() -> str:
    """Reads bootstrap_config.json to find the metadata directory path."""
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../config/bootstrap_config.json"))
    with open(config_path, "r") as f:
        config = json.load(f)
    return config["metadata_path"]

def get_table_path(table_name: str) -> str:
    """Gets the path to a specific metadata Delta table."""
    root = get_metadata_root()
    return os.path.join(root, METADATA_TABLES[table_name]).replace("\\", "/")

def initialize_metadata_tables(spark: SparkSession):
    """Initializes metadata Delta tables with their schemas if they do not exist."""
    tables_to_init = [
        ("pipeline_config", PIPELINE_CONFIG_SCHEMA),
        ("table_config", TABLE_CONFIG_SCHEMA),
        ("column_config", COLUMN_CONFIG_SCHEMA),
        ("dq_rules", DQ_RULES_SCHEMA),
        ("pipeline_dependencies", PIPELINE_DEPENDENCIES_SCHEMA),
        ("watermark_control", WATERMARK_CONTROL_SCHEMA),
        ("audit_pipeline_execution", AUDIT_PIPELINE_EXECUTION_SCHEMA),
        ("audit_pipeline_steps", AUDIT_PIPELINE_STEPS_SCHEMA),
        ("audit_data_reconciliation", AUDIT_DATA_RECONCILIATION_SCHEMA),
        ("data_quality_log", DATA_QUALITY_LOG_SCHEMA)
    ]
    
    for name, schema in tables_to_init:
        path = get_table_path(name)
        if not os.path.exists(path):
            print(f"Initializing empty Delta table '{name}' at {path}")
            spark.createDataFrame(spark.sparkContext.emptyRDD(), schema) \
                .write.format("delta").mode("ignore").save(path)

def get_pipeline_config(spark: SparkSession, pipeline_id: str) -> dict:
    """Reads config by joining pipeline_config and table_config tables."""
    pipe_path = get_table_path("pipeline_config")
    tbl_path = get_table_path("table_config")
    
    pipe_df = spark.read.format("delta").load(pipe_path).filter(col("pipeline_id") == pipeline_id)
    if pipe_df.count() == 0:
        raise ValueError(f"Pipeline '{pipeline_id}' not found in pipeline_config metadata.")
        
    pipe_row = pipe_df.first()
    tbl_df = spark.read.format("delta").load(tbl_path).filter(col("pipeline_id") == pipeline_id)
    
    source_row = tbl_df.filter(col("layer") == "landing").first()
    target_row = tbl_df.filter(col("layer") != "landing").orderBy("layer").first()
    
    if not source_row or not target_row:
        raise ValueError(f"Could not resolve source/target tables for pipeline '{pipeline_id}'.")
        
    return {
        "pipeline_id": pipe_row["pipeline_id"],
        "pipeline_name": pipe_row["pipeline_name"],
        "is_active": pipe_row["is_active"],
        "source_path": source_row["storage_path"],
        "source_format": source_row["file_format"],
        "target_path": target_row["storage_path"],
        "write_mode": target_row["write_mode"],
        "merge_keys": target_row["merge_keys"] if target_row["merge_keys"] else [],
        "partition_cols": target_row["partition_cols"] if target_row["partition_cols"] else []
    }

def get_watermark_config(spark: SparkSession, pipeline_id: str) -> dict:
    """Retrieves the watermark control record for the pipeline."""
    path = get_table_path("watermark_control")
    df = spark.read.format("delta").load(path).filter(col("pipeline_id") == pipeline_id)
    if df.count() == 0:
        return None
    row = df.first()
    return {
        "watermark_id": row["watermark_id"],
        "pipeline_id": row["pipeline_id"],
        "table_id": row["table_id"],
        "watermark_column": row["watermark_column"],
        "last_watermark_value": row["last_watermark_value"],
        "lookback_minutes": row["lookback_minutes"],
        "backfill_status": row["backfill_status"],
        "backfill_start": row["backfill_start"],
        "backfill_end": row["backfill_end"],
        "updated_at": row["updated_at"]
    }

def update_watermark_value(spark: SparkSession, pipeline_id: str, new_value: datetime):
    """Updates last_watermark_value for the pipeline."""
    path = get_table_path("watermark_control")
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, path)
    
    update_row = Row(
        pipeline_id=pipeline_id,
        last_watermark_value=new_value,
        updated_at=datetime.now()
    )
    update_df = spark.createDataFrame([update_row])
    
    delta_table.alias("t").merge(
        update_df.alias("s"),
        "t.pipeline_id = s.pipeline_id"
    ).whenMatchedUpdate(set={
        "last_watermark_value": "s.last_watermark_value",
        "updated_at": "s.updated_at"
    }).execute()

def update_backfill_status(spark: SparkSession, pipeline_id: str, status: str):
    """Updates backfill_status for the pipeline."""
    path = get_table_path("watermark_control")
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, path)
    
    update_row = Row(
        pipeline_id=pipeline_id,
        backfill_status=status,
        updated_at=datetime.now()
    )
    update_df = spark.createDataFrame([update_row])
    
    delta_table.alias("t").merge(
        update_df.alias("s"),
        "t.pipeline_id = s.pipeline_id"
    ).whenMatchedUpdate(set={
        "backfill_status": "s.backfill_status",
        "updated_at": "s.updated_at"
    }).execute()

def log_execution_start(spark: SparkSession, run_id: str, pipeline_id: str, start_time: datetime, watermark_start: datetime):
    """Logs the start of a pipeline run."""
    path = get_table_path("audit_pipeline_execution")
    row = Row(
        run_id=run_id,
        pipeline_id=pipeline_id,
        execution_start=start_time,
        execution_end=None,
        watermark_start=watermark_start,
        watermark_end=None,
        records_read=None,
        records_written=None,
        status="RUNNING",
        error_message=None
    )
    df = spark.createDataFrame([row], schema=AUDIT_PIPELINE_EXECUTION_SCHEMA)
    df.write.format("delta").mode("append").save(path)

def log_execution_end(spark: SparkSession, run_id: str, status: str, end_time: datetime, watermark_end: datetime, records_read: int, records_written: int, error_message: str = None):
    """Updates the execution log entry with results."""
    path = get_table_path("audit_pipeline_execution")
    
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, path)
    
    update_row = Row(
        run_id=run_id,
        pipeline_id="",  
        execution_start=datetime.now(), 
        execution_end=end_time,
        watermark_start=None, 
        watermark_end=watermark_end,
        records_read=int(records_read) if records_read is not None else 0,
        records_written=int(records_written) if records_written is not None else 0,
        status=status,
        error_message=error_message
    )
    
    update_df = spark.createDataFrame([update_row], schema=AUDIT_PIPELINE_EXECUTION_SCHEMA)
    
    delta_table.alias("t").merge(
        update_df.alias("s"),
        "t.run_id = s.run_id"
    ).whenMatchedUpdate(set={
        "execution_end": "s.execution_end",
        "watermark_end": "s.watermark_end",
        "records_read": "s.records_read",
        "records_written": "s.records_written",
        "status": "s.status",
        "error_message": "s.error_message"
    }).execute()

def log_pipeline_step(spark: SparkSession, step_run_id: str, run_id: str, step_name: str, start_time: datetime, end_time: datetime, status: str, records_processed: int, error_details: str = None):
    """Logs sub-step execution details."""
    path = get_table_path("audit_pipeline_steps")
    row = Row(
        step_run_id=step_run_id,
        run_id=run_id,
        step_name=step_name,
        start_time=start_time,
        end_time=end_time,
        status=status,
        records_processed=int(records_processed) if records_processed is not None else 0,
        error_details=error_details
    )
    df = spark.createDataFrame([row], schema=AUDIT_PIPELINE_STEPS_SCHEMA)
    df.write.format("delta").mode("append").save(path)

def log_data_reconciliation(spark: SparkSession, reconciliation_id: str, run_id: str, pipeline_id: str, source_count: int, target_inserted_count: int, target_updated_count: int, quarantine_count: int, discrepancy_count: int, reconciliation_status: str, notes: str):
    """Logs data reconciliation audits."""
    path = get_table_path("audit_data_reconciliation")
    row = Row(
        reconciliation_id=reconciliation_id,
        run_id=run_id,
        pipeline_id=pipeline_id,
        source_count=int(source_count),
        target_inserted_count=int(target_inserted_count),
        target_updated_count=int(target_updated_count),
        quarantine_count=int(quarantine_count),
        discrepancy_count=int(discrepancy_count),
        reconciliation_status=reconciliation_status,
        notes=notes
    )
    df = spark.createDataFrame([row], schema=AUDIT_DATA_RECONCILIATION_SCHEMA)
    df.write.format("delta").mode("append").save(path)

def get_data_quality_rules(spark: SparkSession, pipeline_id: str) -> list:
    """Retrieves active DQ rules."""
    tables_path = get_table_path("table_config")
    rules_path = get_table_path("dq_rules")
    
    if not os.path.exists(tables_path) or not os.path.exists(rules_path):
        return []
        
    tables_df = spark.read.format("delta").load(tables_path).filter(col("pipeline_id") == pipeline_id)
    rules_df = spark.read.format("delta").load(rules_path).filter(col("is_active") == True)
    
    joined_df = rules_df.join(tables_df, "table_id")
    return [row.asDict() for row in joined_df.collect()]

def log_data_quality_result(spark: SparkSession, log_id: str, run_id: str, rule_id: str, failed_records: int, quarantine_path: str, status: str):
    """Logs the results of a data quality check."""
    path = get_table_path("data_quality_log")
    row = Row(
        log_id=log_id,
        run_id=run_id,
        rule_id=rule_id,
        failed_records=int(failed_records),
        quarantine_path=quarantine_path,
        status=status
    )
    df = spark.createDataFrame([row], schema=DATA_QUALITY_LOG_SCHEMA)
    df.write.format("delta").mode("append").save(path)
