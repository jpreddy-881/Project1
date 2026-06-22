import uuid
from datetime import datetime
from pyspark.sql import SparkSession
import src.utils.metadata_helper as meta
from src.utils.logger import StructuredLogger
from src.models.pipeline_context import PipelineContext

class PipelineAuditor:
    """
    Handles logging, step timing, and data reconciliation checks.
    Bound to a PipelineContext for transaction tracing and isolation.
    """
    def __init__(self, spark: SparkSession, pipeline_id: str, context: PipelineContext):
        self.spark = spark
        self.pipeline_id = pipeline_id
        self.context = context
        self.run_id = context.run_id
        self.start_time = context.execution_time
        self.watermark_start = context.watermark
        self.logger = StructuredLogger(f"Pipeline_{pipeline_id}")

    def start(self):
        """Logs the starting status of the execution context."""
        self.logger.info(f"Starting execution for pipeline '{self.pipeline_id}' (Run ID: {self.run_id})")
        meta.log_execution_start(
            spark=self.spark,
            run_id=self.run_id,
            pipeline_id=self.pipeline_id,
            start_time=self.start_time,
            watermark_start=self.watermark_start
        )

    def log_step(self, step_name: str, start_time: datetime, end_time: datetime, status: str, records_processed: int, error_details: str = None):
        """Logs sub-step execution details for performance profiling."""
        step_run_id = str(uuid.uuid4())
        duration = (end_time - start_time).total_seconds()
        msg = f"  Step '{step_name}': {status} in {duration}s. Rows: {records_processed}"
        
        if status == "SUCCESS":
            self.logger.info(msg)
        else:
            self.logger.error(f"{msg}. Error: {error_details}")
            
        meta.log_pipeline_step(
            spark=self.spark,
            step_run_id=step_run_id,
            run_id=self.run_id,
            step_name=step_name,
            start_time=start_time,
            end_time=end_time,
            status=status,
            records_processed=records_processed,
            error_details=error_details
        )

    def reconcile(self, source_count: int, target_inserted_count: int, target_updated_count: int, quarantine_count: int, notes: str = ""):
        """Calculates discrepancy and logs reconciliation status."""
        reconciliation_id = str(uuid.uuid4())
        discrepancy_count = source_count - (target_inserted_count + target_updated_count + quarantine_count)
        
        reconciliation_status = "RECONCILED" if discrepancy_count == 0 else "DISCREPANCY"
        msg = f"  Reconciliation: {reconciliation_status}. Source: {source_count}, Target Inserts: {target_inserted_count}, Target Updates: {target_updated_count}, Quarantine: {quarantine_count}, Discrepancy: {discrepancy_count}"
        
        if reconciliation_status == "RECONCILED":
            self.logger.info(msg)
        else:
            self.logger.warn(msg)
            
        meta.log_data_reconciliation(
            spark=self.spark,
            reconciliation_id=reconciliation_id,
            run_id=self.run_id,
            pipeline_id=self.pipeline_id,
            source_count=source_count,
            target_inserted_count=target_inserted_count,
            target_updated_count=target_updated_count,
            quarantine_count=quarantine_count,
            discrepancy_count=discrepancy_count,
            reconciliation_status=reconciliation_status,
            notes=notes
        )

    def success(self, watermark_end: datetime, records_read: int, records_written: int):
        end_time = datetime.now()
        self.logger.info(f"Pipeline '{self.pipeline_id}' succeeded. Read: {records_read}, Written: {records_written}")
        meta.log_execution_end(
            spark=self.spark,
            run_id=self.run_id,
            status="SUCCESS",
            end_time=end_time,
            watermark_end=watermark_end,
            records_read=records_read,
            records_written=records_written
        )

    def failure(self, error_message: str):
        end_time = datetime.now()
        self.logger.error(f"Pipeline '{self.pipeline_id}' failed. Error: {error_message}")
        meta.log_execution_end(
            spark=self.spark,
            run_id=self.run_id,
            status="FAILED",
            end_time=end_time,
            watermark_end=None,
            records_read=0,
            records_written=0,
            error_message=error_message
        )
