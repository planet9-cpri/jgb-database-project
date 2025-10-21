# scripts/check_table_schemas.py
from google.cloud import bigquery
from pathlib import Path

# 認証情報
credentials_path = r"C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json"
project_id = "jgb2023"
dataset_id = "20251019"

# BigQueryクライアント作成
client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)

# テーブル一覧
tables = ["laws_master", "law_articles_master", "bonds_master"]

for table_name in tables:
    table_ref = f"{project_id}.{dataset_id}.{table_name}"
    table = client.get_table(table_ref)
    
    print(f"\n{'='*60}")
    print(f"📊 {table_name}")
    print(f"{'='*60}")
    
    for field in table.schema:
        nullable = "NULL可" if field.is_nullable else "NOT NULL"
        print(f"  {field.name:30} {field.field_type:15} {nullable}")