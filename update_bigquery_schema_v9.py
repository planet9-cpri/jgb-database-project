"""
BigQuery スキーマ更新スクリプト（v9対応）

プロジェクト情報:
- プロジェクトID: jgb2023
- データセットID: jgb2023_20251029
- ロケーション: asia-northeast1
- 認証ファイル: C:/Users/sonke/secrets/jgb2023-f8c9b849ae2d.json
"""

import os
from google.cloud import bigquery

# 認証情報の設定
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json'

# BigQueryクライアントの初期化（ロケーション指定）
PROJECT_ID = 'jgb2023'
DATASET_ID = 'jgb2023_20251029'
LOCATION = 'asia-northeast1'

client = bigquery.Client(project=PROJECT_ID, location=LOCATION)

# テーブルID
table_id = f"{PROJECT_ID}.{DATASET_ID}.bond_issuances"

print("=" * 80)
print("BigQuery スキーマ更新（v9対応）")
print("=" * 80)
print(f"プロジェクトID: {PROJECT_ID}")
print(f"データセットID: {DATASET_ID}")
print(f"ロケーション: {LOCATION}")
print(f"テーブル: {table_id}")
print("=" * 80)
print()

# スキーマ更新SQL（8つの新フィールドを追加）
schema_updates = [
    {
        'name': 'legal_basis_extracted',
        'description': '抽出された法令参照（元の形式）。例: 財政法第4条第1項'
    },
    {
        'name': 'legal_basis_normalized',
        'description': '正規化された法令キー。例: 財政法第4条第1項'
    },
    {
        'name': 'legal_basis_source',
        'description': '法令情報の抽出元。値: by_law, full_text, bond_name, none'
    },
    {
        'name': 'bond_category',
        'description': '国債種別。例: 建設国債, 借換債, 財投債, 復興債, 政府短期証券'
    },
    {
        'name': 'mof_category',
        'description': '財務省統計上の分類。例: 4条国債, 借換債, 財投債, 復興債, 政府短期証券'
    },
    {
        'name': 'data_quality_score',
        'description': 'データ品質スコア（0-100）。100=最高品質、0=不明'
    },
    {
        'name': 'is_summary_record',
        'description': '総額レコードか。TRUE=総額（集計対象外）、FALSE=個別レコード'
    },
    {
        'name': 'is_detail_record',
        'description': '詳細レコードか。TRUE=個別レコード（集計対象）、FALSE=総額'
    },
]

# 各フィールドを追加
success_count = 0
skip_count = 0
error_count = 0

for field in schema_updates:
    field_name = field['name']
    field_desc = field['description']
    
    # データ型を決定
    if field_name == 'data_quality_score':
        field_type = 'INT64'
    elif field_name in ['is_summary_record', 'is_detail_record']:
        field_type = 'BOOL'
    else:
        field_type = 'STRING'
    
    # ALTER TABLE文を構築
    query = f"""
    ALTER TABLE `{table_id}`
    ADD COLUMN IF NOT EXISTS {field_name} {field_type}
    OPTIONS(description='{field_desc}')
    """
    
    try:
        print(f"[{success_count + skip_count + error_count + 1}/8] フィールド追加中: {field_name} ({field_type})")
        
        # クエリを実行
        query_job = client.query(query, location=LOCATION)
        query_job.result()  # 完了を待つ
        
        print(f"  ✓ 成功: {field_name}")
        success_count += 1
        
    except Exception as e:
        error_msg = str(e)
        
        # "already exists" エラーの場合はスキップ
        if 'already exists' in error_msg.lower() or 'duplicate' in error_msg.lower():
            print(f"  ⊘ スキップ: {field_name}（既に存在）")
            skip_count += 1
        else:
            print(f"  ✗ エラー: {field_name}")
            print(f"    {error_msg}")
            error_count += 1
    
    print()

# 結果サマリー
print("=" * 80)
print("スキーマ更新完了")
print("=" * 80)
print(f"成功: {success_count}件")
print(f"スキップ: {skip_count}件（既存フィールド）")
print(f"エラー: {error_count}件")
print("=" * 80)
print()

# 更新後のスキーマを確認
if success_count > 0 or skip_count > 0:
    print("更新後のスキーマを確認中...")
    print()
    
    verify_query = f"""
    SELECT 
      column_name,
      data_type,
      description
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'bond_issuances'
      AND column_name IN (
        'legal_basis_extracted',
        'legal_basis_normalized',
        'legal_basis_source',
        'bond_category',
        'mof_category',
        'data_quality_score',
        'is_summary_record',
        'is_detail_record'
      )
    ORDER BY ordinal_position
    """
    
    try:
        query_job = client.query(verify_query, location=LOCATION)
        results = query_job.result()
        
        print("v9新フィールド:")
        print("-" * 80)
        for row in results:
            print(f"  {row.column_name:30s} {row.data_type:10s} - {row.description}")
        print("-" * 80)
        print()
        
    except Exception as e:
        print(f"スキーマ確認エラー: {e}")
        print()

# 次のステップ
if error_count == 0:
    print("✓ スキーマ更新が正常に完了しました！")
    print()
    print("次のステップ:")
    print("  1. v9最終改訂版（修正4）のパーサーで10ファイルをテスト実行")
    print("  2. 新フィールドが正しく設定されているか確認")
    print("  3. 全179ファイルを再処理")
    print()
else:
    print("⚠ エラーが発生しました。上記のエラーメッセージを確認してください。")
    print()