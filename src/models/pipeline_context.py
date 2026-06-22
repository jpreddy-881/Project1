import uuid
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class PipelineContext:
    """
    Holds the execution state for a single pipeline run.
    
    Why Pipeline Context is important in production-grade data platforms:
    1. Traceability: Stamping log events, data records, and quarantined records with 
       the unique run_id allows end-to-end debugging of run failures.
    2. Thread-Safety: Keeps execution state isolated from concurrent pipeline runs, 
       avoiding global state bleed.
    3. Simplicity: Avoids passing multiple configuration arguments down the stack 
       by serving as a single unified state container.
    """
    pipeline_name: str
    environment: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    execution_time: datetime = field(default_factory=datetime.now)
    watermark: Optional[datetime] = None
