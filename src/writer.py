import os
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

def write_delta(
    df: DataFrame,
    target_path: str,
    write_mode: str,
    merge_keys: list = None,
    partition_cols: list = None
):
    """
    Writes a PySpark DataFrame to a target Delta table using the specified mode.
    Supports: append, overwrite, and merge.
    """
    # Create parent directories if they don't exist
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    
    # If the target table does not exist, write the initial dataframe to bootstrap the schema
    if not DeltaTable.isDeltaTable(df.sparkSession, target_path):
        writer = df.write.format("delta")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.mode("overwrite").save(target_path)
        return df.count()

    if write_mode == "append":
        writer = df.write.format("delta").mode("append")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(target_path)
        return df.count()
        
    elif write_mode == "overwrite":
        writer = df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(target_path)
        return df.count()
        
    elif write_mode == "merge":
        if not merge_keys:
            raise ValueError("Merge keys must be provided for 'merge' write mode.")
            
        target_table = DeltaTable.forPath(df.sparkSession, target_path)
        
        # Build merge condition: e.g. "t.id = s.id AND t.date = s.date"
        merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
        
        target_table.alias("target") \
            .merge(df.alias("source"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
            
        return df.count()
    else:
        raise ValueError(f"Unsupported write mode: {write_mode}")
