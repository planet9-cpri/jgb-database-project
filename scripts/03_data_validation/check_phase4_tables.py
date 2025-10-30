"""
Phase 4: テーブル作成確認
"""
from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json'

PROJECT_ID = "jgb2023"
DATASET_ID = "20251027"

client = bigquery.Client(project=PROJECT_ID)

print(f"📊 データセット {DATASET_ID} のテーブル一覧")
print("=" * 60)

tables = client.list_tables(f"{PROJECT_ID}.{DATASET_ID}")

table_list = []
for table in tables:
    table_list.append(table.table_id)
    print(f"✅ {table.table_id}")

print("=" * 60)
print(f"合計: {len(table_list)} テーブル")
print()

# 各テーブルのスキーマ確認
for table_id in table_list:
    print(f"\n📋 {table_id} のスキーマ:")
    print("-" * 60)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    table = client.get_table(table_ref)
    
    for field in table.schema:
        print(f"  {field.name:<30} {field.field_type:<15} {field.mode}")
    
    print(f"  合計: {len(table.schema)} フィールド")