import os
import uuid
from typing import List, Tuple, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, count as spark_count
from pyspark.sql.window import Window
from src.models.metadata_models import DQRule
from src.models.pipeline_context import PipelineContext
from src.utils.metadata_helper import log_data_quality_result

class DataQualityEngine:
    """
    Generic Data Quality Engine.
    
    Evaluates active rules defined in metadata (NOT_NULL, UNIQUE, RANGE, REGEX) 
    on PySpark DataFrames, partitions valid and invalid records, gathers metrics,
    and logs outcomes to operational catalogs.
    
    Persists failed records to Delta quarantine tables containing failed rule 
    and execution IDs for lineage traceability.
    """
    def __init__(self, spark: SparkSession, context: PipelineContext):
        self.spark = spark
        self.context = context

    def process(self, df: DataFrame, rules: List[DQRule], pipeline_id: str) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Evaluates data quality rules on the incoming DataFrame.
        
        Returns:
            passed_df: DataFrame containing only records that passed quarantine checks.
            metrics: Dictionary containing pass/fail counts and per-rule failure statistics.
        """
        if not rules:
            return df, {"passed_count": df.count(), "failed_count": 0, "rule_failures": {}}

        clean_df = df
        total_failed_count = 0
        rule_failures = {}

        for rule in rules:
            rule_id = rule.rule_id
            rule_type = rule.rule_type.upper()
            action = rule.action_on_fail.upper()
            column = rule.column_name

            # 1. Resolve filter conditions based on rule type
            if rule_type == "NOT_NULL":
                passed_condition = f"{column} IS NOT NULL"
                failed_condition = f"{column} IS NULL"
                
                passed_df_rule = clean_df.filter(passed_condition)
                failed_df_rule = clean_df.filter(failed_condition)
                
            elif rule_type == "UNIQUE":
                # For UNIQUE, partition by the key column and evaluate occurrences
                w = Window.partitionBy(column)
                df_temp = clean_df.withColumn("_occurrences", spark_count("*").over(w))
                
                passed_df_rule = df_temp.filter(col("_occurrences") == 1).drop("_occurrences")
                failed_df_rule = df_temp.filter(col("_occurrences") > 1).drop("_occurrences")
                
            elif rule_type == "RANGE":
                passed_condition = rule.expression
                failed_condition = f"NOT ({passed_condition}) OR {column} IS NULL"
                
                passed_df_rule = clean_df.filter(passed_condition)
                failed_df_rule = clean_df.filter(failed_condition)
                
            elif rule_type == "REGEX":
                passed_condition = f"{column} RLIKE '{rule.expression}'"
                failed_condition = f"NOT ({passed_condition}) OR {column} IS NULL"
                
                passed_df_rule = clean_df.filter(passed_condition)
                failed_df_rule = clean_df.filter(failed_condition)
                
            else:
                print(f"Warning: Unsupported rule type '{rule_type}' for rule '{rule_id}'. Skipping.")
                continue

            failed_count = failed_df_rule.count()
            rule_failures[rule_id] = failed_count

            if failed_count > 0:
                print(f"Data Quality Rule '{rule_id}' failed for {failed_count} records.")
                
                # Dynamic quarantine folder path resolution
                quarantine_root = os.path.abspath(os.path.join(os.path.dirname(__file__), f"../../data/quarantine/{pipeline_id}/{rule_id}"))
                quarantine_path = quarantine_root.replace("\\", "/")
                
                # Rule Actions Logic
                if action == "FAIL_PIPELINE":
                    log_data_quality_result(self.spark, str(uuid.uuid4()), self.context.run_id, rule_id, failed_count, quarantine_path, "FAILED_HALTED")
                    raise ValueError(f"Pipeline halted: Critical Data Quality Rule '{rule_id}' failed.")
                    
                elif action == "QUARANTINE":
                    # Store failed record, failed rule, and execution id
                    quarantine_df = failed_df_rule \
                        .withColumn("failed_rule", lit(rule_id)) \
                        .withColumn("execution_id", lit(self.context.run_id))
                    
                    os.makedirs(quarantine_path, exist_ok=True)
                    quarantine_df.write.format("delta").mode("append").save(quarantine_path)
                    
                    # Filter out quarantined records from clean pipeline flow
                    clean_df = passed_df_rule
                    total_failed_count += failed_count
                    
                    log_data_quality_result(self.spark, str(uuid.uuid4()), self.context.run_id, rule_id, failed_count, quarantine_path, "FAILED_QUARANTINED")
                    
                elif action == "WARN":
                    log_data_quality_result(self.spark, str(uuid.uuid4()), self.context.run_id, rule_id, failed_count, None, "FAILED_WARN")
            else:
                log_data_quality_result(self.spark, str(uuid.uuid4()), self.context.run_id, rule_id, 0, None, "PASSED")

        metrics = {
            "passed_count": clean_df.count(),
            "failed_count": total_failed_count,
            "rule_failures": rule_failures
        }

        return clean_df, metrics
