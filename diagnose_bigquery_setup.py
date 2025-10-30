"""
BigQuery 環境診断スクリプト

現在のBigQuery環境を調査し、データセットとテーブルの状況を確認します。
"""

import os
from google.cloud import bigquery

# 認証情報の設定
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json'

# プロジェクトID
PROJECT_ID = 'jgb2023'

print("=" * 80)
print("BigQuery 環境診断")
print("=" * 80)
print(f"プロジェクトID: {PROJECT_ID}")
print("=" * 80)
print()

# クライアント初期化（ロケーション指定なし）
client = bigquery.Client(project=PROJECT_ID)

# ステップ1: プロジェクト内の全データセットを列挙
print("ステップ1: データセット一覧")
print("-" * 80)

try:
    datasets = list(client.list_datasets())
    
    if datasets:
        print(f"見つかったデータセット: {len(datasets)}件")
        print()
        for dataset in datasets:
            dataset_id = dataset.dataset_id
            
            # データセットの詳細情報を取得
            dataset_ref = client.dataset(dataset_id)
            dataset_full = client.get_dataset(dataset_ref)
            
            location = dataset_full.location
            created = dataset_full.created
            
            print(f"  データセットID: {dataset_id}")
            print(f"    ロケーション: {location}")
            print(f"    作成日時: {created}")
            
            # このデータセット内のテーブルを列挙
            tables = list(client.list_tables(dataset_id))
            if tables:
                print(f"    テーブル数: {len(tables)}件")
                for table in tables:
                    print(f"      - {table.table_id}")
            else:
                print(f"    テーブル数: 0件")
            print()
    else:
        print("⚠ データセットが見つかりませんでした。")
        print()
        print("考えられる原因:")
        print("  1. データセットがまだ作成されていない")
        print("  2. 認証情報に問題がある")
        print("  3. プロジェクトIDが間違っている")
        print()
        
except Exception as e:
    print(f"✗ エラー: {e}")
    print()

print("-" * 80)
print()

# ステップ2: 特定のデータセットとテーブルをチェック
TARGET_DATASET = 'jgb2023_20251029'
TARGET_TABLE = 'bond_issuances'

print(f"ステップ2: 特定のデータセット確認")
print("-" * 80)
print(f"対象データセット: {TARGET_DATASET}")
print(f"対象テーブル: {TARGET_TABLE}")
print()

try:
    # データセットの存在確認
    dataset_ref = client.dataset(TARGET_DATASET)
    dataset = client.get_dataset(dataset_ref)
    
    print(f"✓ データセット '{TARGET_DATASET}' が見つかりました")
    print(f"  ロケーション: {dataset.location}")
    print(f"  作成日時: {dataset.created}")
    print()
    
    # テーブルの存在確認
    table_ref = dataset_ref.table(TARGET_TABLE)
    
    try:
        table = client.get_table(table_ref)
        print(f"✓ テーブル '{TARGET_TABLE}' が見つかりました")
        print(f"  行数: {table.num_rows:,}行")
        print(f"  スキーマ: {len(table.schema)}フィールド")
        print()
        
        # 現在のスキーマを表示
        print("  現在のフィールド:")
        for field in table.schema:
            print(f"    - {field.name:30s} {field.field_type:10s}")
        print()
        
        # v9新フィールドの存在確認
        v9_fields = [
            'legal_basis_extracted',
            'legal_basis_normalized',
            'legal_basis_source',
            'bond_category',
            'mof_category',
            'data_quality_score',
            'is_summary_record',
            'is_detail_record'
        ]
        
        existing_fields = [f.name for f in table.schema]
        missing_fields = [f for f in v9_fields if f not in existing_fields]
        
        if missing_fields:
            print(f"  ⚠ v9新フィールドが未追加: {len(missing_fields)}件")
            for field in missing_fields:
                print(f"    - {field}")
        else:
            print(f"  ✓ v9新フィールドはすべて追加済み")
        print()
        
    except Exception as e:
        print(f"✗ テーブル '{TARGET_TABLE}' が見つかりません")
        print(f"  エラー: {e}")
        print()
        print("  考えられる原因:")
        print("    1. テーブルがまだ作成されていない")
        print("    2. テーブル名が間違っている")
        print()
        
except Exception as e:
    print(f"✗ データセット '{TARGET_DATASET}' が見つかりません")
    print(f"  エラー: {e}")
    print()
    print("  考えられる原因:")
    print("    1. データセット名が間違っている")
    print("    2. データセットがまだ作成されていない")
    print()

print("-" * 80)
print()

# ステップ3: 推奨される次のアクション
print("ステップ3: 推奨アクション")
print("-" * 80)

if not datasets:
    print("1. データセットを作成してください:")
    print(f"   bq mk --location=asia-northeast1 {PROJECT_ID}:{TARGET_DATASET}")
    print()
    print("2. Layer1テーブル（raw_announcements）を作成")
    print("3. Layer2テーブル（bond_issuances）を作成")
    print()
else:
    # データセットが存在するかチェック
    dataset_exists = any(d.dataset_id == TARGET_DATASET for d in datasets)
    
    if not dataset_exists:
        print(f"1. データセット '{TARGET_DATASET}' を作成してください:")
        print(f"   bq mk --location=asia-northeast1 {PROJECT_ID}:{TARGET_DATASET}")
        print()
    else:
        print(f"✓ データセット '{TARGET_DATASET}' は存在します")
        print()
        print("次のアクション:")
        print("  1. テーブルが存在する場合: スキーマ更新スクリプトを再実行")
        print("  2. テーブルが存在しない場合: テーブルを作成してからスキーマ更新")
        print()

print("=" * 80)
print("診断完了")
print("=" * 80)