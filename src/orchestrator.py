import sys
from datetime import datetime, timedelta
from pyspark.sql.functions import max as spark_max
from delta.tables import DeltaTable
from src.utils.spark_session import get_spark_session
import src.utils.metadata_helper as meta
from src.utils.metadata_manager import MetadataManager
from src.utils.config_manager import ConfigManager
from src.utils.watermark_manager import WatermarkManager
from src.engine.ingestion_engine import IngestionEngine
from src.engine.dq_engine import DataQualityEngine
from src.models.pipeline_context import PipelineContext
from src.audit import PipelineAuditor
from src.writer import write_delta

def run_pipeline(
    pipeline_id: str,
    watermark_col: str = "last_updated_at"
):
    """
    Coordinates and runs a Lakehouse ETL pipeline using metadata.
    Uses a dedicated WatermarkManager, IngestionEngine, and DataQualityEngine to coordinate runs.
    """
    spark = get_spark_session(f"Orchestrator_{pipeline_id}")
    
    # Initialize control tables if not already present
    meta.initialize_metadata_tables(spark)
    
    # Retrieve pipeline config via MetadataManager
    meta_mgr = MetadataManager(spark)
    pipeline_cfg = meta_mgr.get_pipeline(pipeline_id)
    if not pipeline_cfg.is_active:
        print(f"Pipeline '{pipeline_id}' is inactive. Skipping.")
        return
        
    tables = meta_mgr.get_table_config(pipeline_id)
    source_cfg = next((t for t in tables if t.layer == "landing"), None)
    target_cfg = next((t for t in tables if t.layer != "landing"), None)
    
    if not source_cfg or not target_cfg:
        raise ValueError(f"Could not resolve source/target tables for pipeline '{pipeline_id}'.")
        
    # Query watermark control config via WatermarkManager
    wtm_mgr = WatermarkManager(spark)
    wtm_cfg = wtm_mgr.get_watermark_config(pipeline_id)
    is_backfill_mode = False
    
    if wtm_cfg:
        watermark_col = wtm_cfg["watermark_column"]
        if wtm_cfg["backfill_status"] == "ACTIVE":
            print(f"Active Backfill Detected! Range: {wtm_cfg['backfill_start']} to {wtm_cfg['backfill_end']}")
            watermark_start = wtm_cfg["backfill_start"]
            watermark_end = wtm_cfg["backfill_end"]
            is_backfill_mode = True
        else:
            last_val = wtm_cfg["last_watermark_value"]
            lookback = wtm_cfg["lookback_minutes"]
            
            if last_val and lookback > 0:
                watermark_start = last_val - timedelta(minutes=int(lookback))
                print(f"Incremental Run with Lookback. Value: {last_val}, Effective Start (Lookback: {lookback}m): {watermark_start}")
            else:
                watermark_start = last_val
                print(f"Incremental Run. Start Watermark: {watermark_start}")
            
            watermark_end = datetime.now()
    else:
        # Fallback if no watermark control record exists
        watermark_start = None
        watermark_end = datetime.now()
        
    # Initialize pipeline context and auditor
    env = ConfigManager().env
    context = PipelineContext(
        pipeline_name=pipeline_cfg.pipeline_name,
        environment=env,
        watermark=watermark_start
    )
    
    auditor = PipelineAuditor(spark, pipeline_id, context)
    auditor.start()
    
    try:
        # 1. Read / Extract Step via generic IngestionEngine
        step_start = datetime.now()
        engine = IngestionEngine(spark, context)
        df_source = engine.ingest(
            source_config=source_cfg,
            watermark_col=watermark_col,
            watermark_start=watermark_start,
            watermark_end=watermark_end
        )
        records_read = df_source.count()
        step_end = datetime.now()
        auditor.log_step("extract", step_start, step_end, "SUCCESS", records_read)
        
        if records_read == 0:
            print("No new records to process.")
            auditor.reconcile(0, 0, 0, 0, "No source records read.")
            
            if is_backfill_mode:
                meta.update_backfill_status(spark, pipeline_id, "COMPLETED")
                
            auditor.success(
                watermark_end=watermark_end if is_backfill_mode else watermark_start,
                records_read=0,
                records_written=0
            )
            return
 
        # 2. Schema / Quality checks Step via generic DataQualityEngine
        step_start = datetime.now()
        rules = meta_mgr.get_dq_rules(pipeline_id)
        dq_engine = DataQualityEngine(spark, context)
        df_clean, dq_metrics = dq_engine.process(
            df=df_source,
            rules=rules,
            pipeline_id=pipeline_id
        )
        records_clean = dq_metrics["passed_count"]
        quarantine_count = dq_metrics["failed_count"]
        step_end = datetime.now()
        auditor.log_step("dq_checks", step_start, step_end, "SUCCESS", records_clean)
        
        # Calculate inserts vs updates for reconciliation
        target_path = target_cfg.storage_path
        target_exists = DeltaTable.isDeltaTable(spark, target_path)
        
        target_inserted_count = records_clean
        target_updated_count = 0
        
        if target_exists and target_cfg.write_mode == "merge" and target_cfg.merge_keys:
            target_df = spark.read.format("delta").load(target_path)
            target_inserted_count = df_clean.join(target_df, target_cfg.merge_keys, "leftanti").count()
            target_updated_count = records_clean - target_inserted_count
 
        # 3. Write / Load Step
        step_start = datetime.now()
        records_written = write_delta(
            df=df_clean,
            target_path=target_path,
            write_mode=target_cfg.write_mode,
            merge_keys=target_cfg.merge_keys,
            partition_cols=target_cfg.partition_cols
        )
        step_end = datetime.now()
        auditor.log_step("target_load", step_start, step_end, "SUCCESS", records_written)
        
        # 4. Reconcile Data Counts
        notes = f"Processed {records_read} source rows: {target_inserted_count} inserts, {target_updated_count} updates, {quarantine_count} quarantined."
        if is_backfill_mode:
            notes += " Mode: Backfill."
        auditor.reconcile(
            source_count=records_read,
            target_inserted_count=target_inserted_count,
            target_updated_count=target_updated_count,
            quarantine_count=quarantine_count,
            notes=notes
        )
        
        # Capture maximum timestamp as watermark
        if watermark_col in df_clean.columns:
            max_ts_row = df_clean.select(spark_max(watermark_col)).first()
            if max_ts_row and max_ts_row[0]:
                val = max_ts_row[0]
                if isinstance(val, str):
                    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                        try:
                            watermark_end = datetime.strptime(val, fmt)
                            break
                        except ValueError:
                            pass
                else:
                    watermark_end = val
        
        # Update watermark tracking state via WatermarkManager
        if wtm_cfg:
            if is_backfill_mode:
                print(f"Backfill complete. Setting backfill status to COMPLETED.")
                meta.update_backfill_status(spark, pipeline_id, "COMPLETED")
            else:
                print(f"Updating watermark value to: {watermark_end}")
                wtm_mgr.update_watermark(pipeline_id, watermark_end)
        
        # 5. Audit Log Success
        auditor.success(
            watermark_end=watermark_end,
            records_read=records_read,
            records_written=records_written
        )
        
    except Exception as e:
        auditor.failure(str(e))
        raise e
