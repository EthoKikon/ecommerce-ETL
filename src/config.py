# centralize config and keep secrets out of code
import os
from pathlib import Path

from dotenv import load_dotenv

# read .env in project root
load_dotenv()

# Export simple config variables
DATABASE_URL = os.getenv("DATABASE_URL")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "outputs"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Ensure output dir exists when module is imported
try:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    pass  # if for any reason we cannot create this dir, don't crash import
