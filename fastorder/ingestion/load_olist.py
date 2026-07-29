import pandas 
from pathlib import Path
from fastorder.ingestion.db import get_connection
import time

project_root = Path(__file__).resolve().parents[2]
data_dir = project_root / "data" / "raw" / "olist"

