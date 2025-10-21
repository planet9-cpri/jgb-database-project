"""
プロジェクト設定
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# .envファイルを読み込み
load_dotenv()

# プロジェクトルート
PROJECT_ROOT = Path(__file__).parent.parent

# BigQuery設定
BIGQUERY_CONFIG = {
    "project_id": os.getenv("BIGQUERY_PROJECT_ID", "jgb2023"),
    "dataset_id": os.getenv("BIGQUERY_DATASET_ID", "20251019"),
    "credentials_path": os.getenv(
        "BIGQUERY_CREDENTIALS_PATH", 
        r"C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json"
    ),
    "location": os.getenv("BIGQUERY_LOCATION", "asia-northeast1")
}

# データパス
DATA_PATHS = {
    "kanpo_2023_dir": Path(os.getenv("KANPO_2023_DIR", r"G:\マイドライブ\JGBデータ\2023")),
    "output_dir": PROJECT_ROOT / os.getenv("OUTPUT_DIR", "output"),
    "masters_dir": PROJECT_ROOT / os.getenv("MASTERS_DIR", "data/masters"),
    "logs_dir": PROJECT_ROOT / os.getenv("LOG_DIR", "logs")
}

# ログ設定
LOGGING_CONFIG = {
    "level": os.getenv("LOG_LEVEL", "INFO"),
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "log_file": DATA_PATHS["logs_dir"] / "jgb_database.log"
}

# テーブル名
TABLE_NAMES = {
    "laws_master": "laws_master",
    "law_articles_master": "law_articles_master",
    "bonds_master": "bonds_master",
    "announcements": "announcements",
    "bond_issuances": "bond_issuances",
    "issuance_legal_basis": "issuance_legal_basis"
}
