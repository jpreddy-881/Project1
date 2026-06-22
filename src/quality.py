import uuid
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, lit
from src.utils.metadata_helper import log_data_quality_result

def run_quality_checks(
    df: DataFrame,
    run_id: str,
    rules: list,
    pipeline_id: str
) -> DataFrame:
    """
    Evaluates data quality rules on the incoming DataFrame.
    Valid records are returned; invalid records are quarantined or trigger alerts.
    """
    if not rules:
        return df

    spark = df.sparkSession
    clean_df = df
    
    # We will build a conjoined expression representing all rule passes to find clean rows
    for rule in rules:
        if hasattr(rule, "rule_id"):
            rule_id = rule.rule_id
            expression = rule.expression
            action = rule.action_on_fail
        else:
            rule_id = rule["rule_id"]
            expression = rule["expression"]
            action = rule["action_on_fail"]
        
        # Evaluate condition: condition evaluates to True for passed rows
        failed_df = df.filter(f"NOT ({expression})")
        failed_count = failed_df.count()
        
        if failed_count > 0:
            print(f"Data Quality Rule '{rule_id}' failed for {failed_count} records.")
            
            # Setup quarantine path
            quarantine_root = os.path.abspath(os.path.join(os.path.dirname(__file__), f"../data/quarantine/{pipeline_id}/{rule_id}"))
            quarantine_path = quarantine_root.replace("\\", "/")
            
            # Action logic
            if action == "FAIL_PIPELINE":
                log_data_quality_result(spark, str(uuid.uuid4()), run_id, rule_id, failed_count, quarantine_path, "FAILED_HALTED")
                raise ValueError(f"Pipeline halted: Data Quality Rule '{rule_id}' failed with critical status.")
                
            elif action == "QUARANTINE":
                # Save invalid rows to quarantine
                os.makedirs(quarantine_path, exist_ok=True)
                failed_df.write.format("delta").mode("append").save(quarantine_path)
                
                # Filter out the invalid rows from our clean dataset
                clean_df = clean_df.filter(expression)
                
                log_data_quality_result(spark, str(uuid.uuid4()), run_id, rule_id, failed_count, quarantine_path, "FAILED_QUARANTINED")
                
            elif action == "WARN":
                log_data_quality_result(spark, str(uuid.uuid4()), run_id, rule_id, failed_count, None, "FAILED_WARN")
        else:
            log_data_quality_result(spark, str(uuid.uuid4()), run_id, rule_id, 0, None, "PASSED")
            
    return clean_df
