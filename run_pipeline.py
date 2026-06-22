import os
import sys
import shutil
import json
from datetime import datetime, timedelta
from pyspark.sql import Row

# Set environment variables for PySpark worker compatibility
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Add current path to python path to import modules
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from src.utils.spark_session import get_spark_session
import src.utils.metadata_helper as meta
from src.orchestrator import run_pipeline

def setup_mock_landing_data(landing_path: str):
    """Creates initial mock landing JSON data."""
    os.makedirs(landing_path, exist_ok=True)
    
    # 1. First batch of clean data
    batch_1 = [
        {"sale_id": "S001", "product": "Laptop", "unit_price": 1200.0, "quantity": 1, "last_updated_at": "2026-06-20T10:00:00"},
        {"sale_id": "S002", "product": "Mouse", "unit_price": 25.0, "quantity": 2, "last_updated_at": "2026-06-20T10:05:00"},
        {"sale_id": "S003", "product": "Monitor", "unit_price": 300.0, "quantity": 1, "last_updated_at": "2026-06-20T10:10:00"}
    ]
    
    with open(os.path.join(landing_path, "batch_1.json"), "w") as f:
        for record in batch_1:
            f.write(json.dumps(record) + "\n")
            
    print(f"Created Batch 1 landing data at {landing_path}/batch_1.json")

def append_second_batch(landing_path: str):
    """Appends a second batch containing normal updates and an invalid row for quality testing."""
    # S002 is an update (to test merge), S004 is a clean new record (with a duplicate), S005 is invalid (price < 0)
    batch_2 = [
        {"sale_id": "S002", "product": "Mouse", "unit_price": 20.0, "quantity": 3, "last_updated_at": "2026-06-20T11:00:00"},
        {"sale_id": "S004", "product": "Keyboard", "unit_price": 75.0, "quantity": 1, "last_updated_at": "2026-06-20T11:15:00"},
        {"sale_id": "S004", "product": "Duplicate Keyboard", "unit_price": 75.0, "quantity": 1, "last_updated_at": "2026-06-20T11:16:00"}, # Duplicate sale_id for UNIQUE check
        {"sale_id": "S005", "product": "Broken Keyboard", "unit_price": -10.0, "quantity": 1, "last_updated_at": "2026-06-20T11:30:00"} # Price < 0 for RANGE check
    ]
    
    with open(os.path.join(landing_path, "batch_2.json"), "w") as f:
        for record in batch_2:
            f.write(json.dumps(record) + "\n")
            
    print(f"Created Batch 2 landing data (including an invalid record) at {landing_path}/batch_2.json")

def seed_metadata(spark):
    """Seeds the pipeline metadata and data quality rules tables using the new schemas."""
    meta.initialize_metadata_tables(spark)
    
    pipeline_id = "sales_landing_to_bronze"
    landing_path = os.path.abspath("./data/landing/raw_sales").replace("\\", "/")
    bronze_path = os.path.abspath("./data/bronze/db_bronze/sales").replace("\\", "/")
    
    # 1. Seed pipeline_config
    pipeline_df = spark.createDataFrame([
        Row(
            pipeline_id=pipeline_id,
            pipeline_name="Sales Ingestion Pipeline",
            description="Extracts raw sales JSON from landing and writes to Medallion Bronze",
            schedule_cron="0 * * * *",
            is_active=True,
            created_at=datetime.now()
        )
    ], schema=meta.PIPELINE_CONFIG_SCHEMA)
    pipeline_df.write.format("delta").mode("overwrite").save(meta.get_table_path("pipeline_config"))
    print("Seeded pipeline_config.")
    
    # 2. Seed table_config (landing source and bronze target)
    tables_df = spark.createDataFrame([
        Row(
            table_id="sales_landing_source",
            pipeline_id=pipeline_id,
            layer="landing",
            table_name="raw_sales",
            storage_path=landing_path,
            file_format="json",
            write_mode="append",
            partition_cols=[],
            merge_keys=[]
        ),
        Row(
            table_id="sales_bronze_target",
            pipeline_id=pipeline_id,
            layer="bronze",
            table_name="sales",
            storage_path=bronze_path,
            file_format="delta",
            write_mode="merge",
            partition_cols=[],
            merge_keys=["sale_id"]
        )
    ], schema=meta.TABLE_CONFIG_SCHEMA)
    tables_df.write.format("delta").mode("overwrite").save(meta.get_table_path("table_config"))
    print("Seeded table_config.")

    # 3. Seed column_config for sales conformed schema
    columns_df = spark.createDataFrame([
        Row(column_id="col_sale_id", table_id="sales_bronze_target", column_name="sale_id", data_type="string", is_nullable=False, is_primary_key=True, description="Unique sale identifier"),
        Row(column_id="col_product", table_id="sales_bronze_target", column_name="product", data_type="string", is_nullable=True, is_primary_key=False, description="Product name"),
        Row(column_id="col_unit_price", table_id="sales_bronze_target", column_name="unit_price", data_type="double", is_nullable=True, is_primary_key=False, description="Unit price"),
        Row(column_id="col_quantity", table_id="sales_bronze_target", column_name="quantity", data_type="long", is_nullable=True, is_primary_key=False, description="Quantity sold"),
        Row(column_id="col_last_updated_at", table_id="sales_bronze_target", column_name="last_updated_at", data_type="string", is_nullable=True, is_primary_key=False, description="Updated timestamp string")
    ], schema=meta.COLUMN_CONFIG_SCHEMA)
    columns_df.write.format("delta").mode("overwrite").save(meta.get_table_path("column_config"))
    print("Seeded column_config.")
    
    # 4. Seed dq_rules (linked to target table_id)
    rules_df = spark.createDataFrame([
        Row(
            rule_id="sales_price_range",
            table_id="sales_bronze_target",
            column_name="unit_price",
            rule_type="RANGE",
            expression="unit_price > 0",
            action_on_fail="QUARANTINE",
            is_active=True
        ),
        Row(
            rule_id="sales_id_not_null",
            table_id="sales_bronze_target",
            column_name="sale_id",
            rule_type="NOT_NULL",
            expression="",
            action_on_fail="QUARANTINE",
            is_active=True
        ),
        Row(
            rule_id="sales_id_regex",
            table_id="sales_bronze_target",
            column_name="sale_id",
            rule_type="REGEX",
            expression="^S\\d+$",
            action_on_fail="QUARANTINE",
            is_active=True
        ),
        Row(
            rule_id="sales_id_unique",
            table_id="sales_bronze_target",
            column_name="sale_id",
            rule_type="UNIQUE",
            expression="",
            action_on_fail="QUARANTINE",
            is_active=True
        )
    ], schema=meta.DQ_RULES_SCHEMA)
    rules_df.write.format("delta").mode("overwrite").save(meta.get_table_path("dq_rules"))
    print("Seeded dq_rules.")
    
    # 5. Seed pipeline_dependencies (empty for this example pipeline)
    dependencies_df = spark.createDataFrame([], schema=meta.PIPELINE_DEPENDENCIES_SCHEMA)
    dependencies_df.write.format("delta").mode("overwrite").save(meta.get_table_path("pipeline_dependencies"))
    print("Seeded pipeline_dependencies.")
    
    # 6. Seed watermark_control
    watermark_df = spark.createDataFrame([
        Row(
            watermark_id="sales_watermark_001",
            pipeline_id=pipeline_id,
            table_id="sales_bronze_target",
            watermark_column="last_updated_at",
            last_watermark_value=None,
            lookback_minutes=0,
            backfill_status="NONE",
            backfill_start=None,
            backfill_end=None,
            updated_at=datetime.now()
        )
    ], schema=meta.WATERMARK_CONTROL_SCHEMA)
    watermark_df.write.format("delta").mode("overwrite").save(meta.get_table_path("watermark_control"))
    print("Seeded watermark_control.")

def main():
    # Set target environment profile
    os.environ["ENV"] = "test"
    from src.utils.config_manager import ConfigManager
    config_mgr = ConfigManager()
    
    # Clean workspace directories first to ensure fresh run
    # Read directories dynamically from config
    paths_to_clean = ["./config/bootstrap_config.json", "./data/metadata_test", "./data/warehouse_test", "./data/quarantine", "./data/landing"]
    for p in paths_to_clean:
        if os.path.exists(p):
            if os.path.isdir(p):
                shutil.rmtree(p)
            else:
                os.remove(p)
                
    # Restore bootstrap config file dynamically
    os.makedirs("./config", exist_ok=True)
    with open("./config/bootstrap_config.json", "w") as f:
        json.dump({"metadata_path": config_mgr.get("metadata_path")}, f)
        
    spark = get_spark_session("LakehouseVerification")
    
    # Verify SparkSessionManager Singleton behavior
    from src.utils.spark_session import SparkSessionManager
    spark_session_1 = SparkSessionManager("LakehouseVerification").spark
    spark_session_2 = SparkSessionManager("AnotherName").spark
    print(f"\n--- VERIFYING SINGLETON SPARK SESSION ---")
    print(f"Session 1: {spark_session_1}")
    print(f"Session 2: {spark_session_2}")
    print(f"Are session instances identical? {spark_session_1 is spark_session_2}")
    assert spark_session_1 is spark_session_2, "SparkSessionManager is NOT a Singleton!"
    
    landing_path = os.path.abspath("./data/landing/raw_sales")
    setup_mock_landing_data(landing_path)
    
    # Seed metadata
    seed_metadata(spark)
    
    print("\n--- RUNNING BATCH 1 (INITIAL RUN) ---")
    run_pipeline("sales_landing_to_bronze")
    
    print("\n--- VERIFYING BRONZE TARGET TABLE AFTER BATCH 1 ---")
    bronze_path = os.path.abspath("./data/bronze/db_bronze/sales").replace("\\", "/")
    spark.read.format("delta").load(bronze_path).show()
    
    # Append second batch
    append_second_batch(landing_path)
    
    print("\n--- RUNNING BATCH 2 (INCREMENTAL MERGE + QUALITY QUARANTINE) ---")
    run_pipeline("sales_landing_to_bronze")
    
    print("\n--- VERIFYING BRONZE TARGET TABLE AFTER BATCH 2 ---")
    spark.read.format("delta").load(bronze_path).show()
    
    print("\n--- TRIGGERING BACKFILL RUN (FOR RANGE 2026-06-20 10:00:00 TO 10:10:00) ---")
    # Set backfill status to ACTIVE and configure start/end bounds
    meta.update_backfill_status(spark, "sales_landing_to_bronze", "ACTIVE")
    
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, meta.get_table_path("watermark_control"))
    delta_table.update(
        condition="pipeline_id = 'sales_landing_to_bronze'",
        set={
            "backfill_start": "CAST('2026-06-20 10:00:00' AS timestamp)",
            "backfill_end": "CAST('2026-06-20 10:10:00' AS timestamp)"
        }
    )
    
    # Run pipeline under backfill mode
    run_pipeline("sales_landing_to_bronze")
    
    print("\n--- VERIFYING QUARANTINED DATA ---")
    for rule in ("sales_price_range", "sales_id_unique"):
        qp = os.path.abspath(f"./data/quarantine/sales_landing_to_bronze/{rule}").replace("\\", "/")
        print(f"\nQuarantine folder for rule '{rule}':")
        if os.path.exists(qp):
            spark.read.format("delta").load(qp).show()
        else:
            print("  No quarantined records folder found!")
        
    print("\n--- VERIFYING WATERMARK CONTROL STATE (watermark_control) ---")
    spark.read.format("delta").load(meta.get_table_path("watermark_control")).show(truncate=False)
        
    print("\n--- VERIFYING EXECUTION AUDITS (audit_pipeline_execution) ---")
    spark.read.format("delta").load(meta.get_table_path("audit_pipeline_execution")).show(truncate=False)

    print("\n--- VERIFYING STEP AUDITS (audit_pipeline_steps) ---")
    spark.read.format("delta").load(meta.get_table_path("audit_pipeline_steps")).orderBy("start_time").show(truncate=False)

    print("\n--- VERIFYING DATA RECONCILIATION AUDITS (audit_data_reconciliation) ---")
    spark.read.format("delta").load(meta.get_table_path("audit_data_reconciliation")).show(truncate=False)
    
    print("\n--- VERIFYING QUALITY LOGS ---")
    spark.read.format("delta").load(meta.get_table_path("data_quality_log")).show(truncate=False)
    
    if sys.stdin.isatty():
        input("\n=== Spark Session is active. Open http://localhost:4040 in your browser, then press Enter here to exit... ===")
    spark.stop()

if __name__ == "__main__":
    main()
