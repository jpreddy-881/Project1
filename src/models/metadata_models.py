from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

@dataclass
class PipelineConfig:
    pipeline_id: str
    pipeline_name: str
    description: Optional[str]
    schedule_cron: Optional[str]
    is_active: bool
    created_at: datetime

@dataclass
class TableConfig:
    table_id: str
    pipeline_id: str
    layer: str
    table_name: str
    storage_path: str
    file_format: str
    write_mode: str
    partition_cols: List[str] = field(default_factory=list)
    merge_keys: List[str] = field(default_factory=list)

@dataclass
class ColumnConfig:
    column_id: str
    table_id: str
    column_name: str
    data_type: str
    is_nullable: bool
    is_primary_key: bool
    description: Optional[str]

@dataclass
class DQRule:
    rule_id: str
    table_id: str
    column_name: str
    rule_type: str
    expression: str
    action_on_fail: str
    is_active: bool
