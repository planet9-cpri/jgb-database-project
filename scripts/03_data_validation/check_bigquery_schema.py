"""
BigQuery テーブルスキーマ確認スクリプト

使用方法:
    python check_bigquery_schema.py
"""

from google.cloud import bigquery
from google.oauth2 import service_account

# 設定
PROJECT_ID = "jgb2023"
DATASET_ID = "20251025"
TABLE_ID = "bond_issuances"
CREDENTIALS_PATH = r"C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json"

def check_schema():
    """テーブルスキーマを確認"""
    print("=" * 80)
    print("BigQuery テーブルスキーマ確認")
    print("=" * 80)
    
    # クライアント初期化
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    
    client = bigquery.Client(
        credentials=credentials,
        project=PROJECT_ID,
    )
    
    # テーブル参照
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    print(f"テーブル: {table_ref}")
    print("")
    
    try:
        # テーブル取得
        table = client.get_table(table_ref)
        
        print("【スキーマ】")
        print("-" * 80)
        for i, field in enumerate(table.schema, 1):
            nullable = "NULL可" if field.is_nullable else "NOT NULL"
            print(f"{i:2d}. {field.name:30s} {field.field_type:15s} {nullable}")
        
        print("-" * 80)
        print(f"総フィールド数: {len(table.schema)}")
        
        # テーブル情報
        print("\n【テーブル情報】")
        print(f"行数: {table.num_rows}")
        print(f"サイズ: {table.num_bytes / 1024 / 1024:.2f} MB")
        print(f"作成日: {table.created}")
        print(f"最終更新: {table.modified}")
        
    except Exception as e:
        print(f"❌ エラー: {e}")
        print("\nテーブルが存在しない可能性があります")
        print("データセット内のテーブル一覧を確認します...")
        
        # データセット内のテーブル一覧
        try:
            dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
            tables = client.list_tables(dataset_ref)
            
            print(f"\n【データセット {DATASET_ID} 内のテーブル】")
            for table in tables:
                print(f"  - {table.table_id}")
        except Exception as e2:
            print(f"❌ データセット確認エラー: {e2}")
    
    print("=" * 80)


if __name__ == "__main__":
    check_schema()