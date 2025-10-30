"""
テーブルを削除して正しいスキーマで再作成するスクリプト

使用方法:
    python scripts/recreate_tables.py
"""

import os
from google.cloud import bigquery

PROJECT_ID = "jgb2023"
DATASET_ID = "20251019"
SERVICE_ACCOUNT_KEY = r"C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json"

def recreate_tables():
    """テーブルを再作成"""
    print("="*60)
    print("🔨 テーブルの再作成")
    print("="*60)
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY
    client = bigquery.Client(project=PROJECT_ID)
    
    # トランザクションテーブルのみ再作成（マスタは保持）
    tables_config = {
        'announcements': [
            bigquery.SchemaField("announcement_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("kanpo_date", "DATE"),
            bigquery.SchemaField("announcement_number", "STRING"),
            bigquery.SchemaField("gazette_issue_number", "STRING"),  # ← 追加
            bigquery.SchemaField("announcement_type", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("source_file", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ],
        'bond_issuances': [
            bigquery.SchemaField("issuance_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("announcement_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("bond_master_id", "STRING"),
            bigquery.SchemaField("issuance_date", "DATE"),
            bigquery.SchemaField("maturity_date", "DATE"),
            bigquery.SchemaField("interest_rate", "FLOAT64"),
            bigquery.SchemaField("issue_price", "FLOAT64"),
            bigquery.SchemaField("issue_amount", "INT64"),
            bigquery.SchemaField("payment_date", "DATE"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ],
        'issuance_legal_basis': [
            bigquery.SchemaField("issuance_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("law_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("article_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]
    }
    
    for table_name, schema in tables_config.items():
        print(f"\n{'='*60}")
        print(f"📊 {table_name}")
        print("="*60)
        
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        
        # 既存テーブルの確認
        try:
            existing_table = client.get_table(table_id)
            row_count = existing_table.num_rows
            
            print(f"  ℹ️ 既存テーブルあり: {row_count:,} 行")
            
            if row_count > 0:
                response = input(f"  ⚠️ データが含まれています。削除しますか？ (yes/no): ")
                if response.lower() != 'yes':
                    print(f"  ⏭️ スキップ")
                    continue
            
            # テーブルを削除
            print(f"  🗑️ 既存テーブルを削除中...")
            client.delete_table(table_id)
            print(f"  ✅ 削除完了")
            
        except Exception:
            print(f"  ℹ️ 既存テーブルなし（新規作成）")
        
        # テーブルを作成
        print(f"  🔨 新しいテーブルを作成中...")
        
        table = bigquery.Table(table_id, schema=schema)
        
        # パーティション設定（created_atで日次パーティション）
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_at"
        )
        
        try:
            table = client.create_table(table)
            print(f"  ✅ テーブル作成成功")
            print(f"  📊 カラム数: {len(schema)}")
            print(f"  📅 パーティション: DAY (created_at)")
            
        except Exception as e:
            print(f"  ❌ テーブル作成失敗: {e}")
    
    print("\n" + "="*60)
    print("✅ 完了")
    print("="*60)
    
    print("\n【次のステップ】")
    print("  python scripts/load_issuance_data.py --limit 1")


if __name__ == "__main__":
    recreate_tables()