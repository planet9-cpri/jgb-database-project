"""
既存テーブルのスキーマを確認し、必要な変更を提案するスクリプト

使用方法:
    python scripts/check_table_schema.py
"""

import os
from google.cloud import bigquery

PROJECT_ID = "jgb2023"
DATASET_ID = "20251019"
SERVICE_ACCOUNT_KEY = r"C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json"

def check_schemas():
    """テーブルスキーマを確認"""
    print("="*60)
    print("🔍 テーブルスキーマの確認")
    print("="*60)
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY
    client = bigquery.Client(project=PROJECT_ID)
    
    tables_to_check = ['announcements', 'bond_issuances', 'issuance_legal_basis']
    
    for table_name in tables_to_check:
        print(f"\n{'='*60}")
        print(f"📊 {table_name}")
        print("="*60)
        
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        
        try:
            table = client.get_table(table_id)
            
            print(f"\n【基本情報】")
            print(f"  テーブルID: {table.table_id}")
            print(f"  行数: {table.num_rows:,}")
            print(f"  作成日: {table.created}")
            
            print(f"\n【スキーマ】")
            for field in table.schema:
                mode = f"[{field.mode}]" if field.mode != "NULLABLE" else ""
                print(f"  {field.name:30s} {field.field_type:15s} {mode}")
            
            print(f"\n【カラム数】 {len(table.schema)}")
            
        except Exception as e:
            print(f"  ❌ エラー: {e}")
    
    print("\n" + "="*60)
    print("📋 推奨される対応")
    print("="*60)
    
    print("\n【オプションA: テーブルを削除して再作成（推奨）】")
    print("  1. 既存データのバックアップ（必要に応じて）")
    print("  2. テーブルを削除")
    print("  3. 正しいスキーマで再作成")
    print("  4. マスタデータを再投入")
    print("  5. 発行データを投入")
    
    print("\n【オプションB: カラムを追加】")
    print("  1. ALTER TABLE で不足カラムを追加")
    print("  2. データを投入")


if __name__ == "__main__":
    check_schemas()