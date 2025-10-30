"""
Phase 4: データセット作成
"""
from google.cloud import bigquery
import os

# 認証設定
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json'

PROJECT_ID = "jgb2023"
DATASET_ID = "20251027"

client = bigquery.Client(project=PROJECT_ID)

# データセット作成
dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
dataset = bigquery.Dataset(dataset_ref)
dataset.location = "asia-northeast1"  # 東京リージョン
dataset.description = "Phase 4: 3-layer architecture for JGB announcements"

try:
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"✅ Dataset {DATASET_ID} created successfully in {dataset.location}")
except Exception as e:
    print(f"❌ Error: {e}")