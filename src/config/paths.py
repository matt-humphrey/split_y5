from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[2]

DATA = PROJECT_ROOT / 'data'
OG_DATA = DATA / 'original'
RAW_DATA = DATA / 'raw'
INTERIM_DATA = DATA / 'interim'
PROCESSED_DATA = DATA / 'processed'