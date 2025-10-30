"""
BigQuery テーブル作成スクリプト（v9対応）

データセット: 20251029
ロケーション: asia-northeast1
"""

import os
from google.cloud import bigquery

# 認証情報の設定
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json'

# プロジェクト情報
PROJECT_ID = 'jgb2023'
DATASET_ID = '20251029'
LOCATION = 'asia-northeast1'

client = bigquery.Client(project=PROJECT_ID, location=LOCATION)

print("=" * 80)
print("BigQuery テーブル作成（v9対応）")
print("=" * 80)
print(f"プロジェクトID: {PROJECT_ID}")
print(f"データセットID: {DATASET_ID}")
print(f"ロケーション: {LOCATION}")
print("=" * 80)
print()

# Layer1テーブル: raw_announcements
print("Layer1テーブル作成中: raw_announcements")
print("-" * 80)

raw_announcements_schema = [
    bigquery.SchemaField("announcement_id", "STRING", mode="REQUIRED", description="告示ID（一意識別子）"),
    bigquery.SchemaField("file_path", "STRING", mode="REQUIRED", description="ファイルパス"),
    bigquery.SchemaField("file_name", "STRING", description="ファイル名"),
    bigquery.SchemaField("announcement_date", "DATE", description="告示日"),
    bigquery.SchemaField("announcement_number", "STRING", description="告示番号"),
    bigquery.SchemaField("title", "STRING", description="タイトル"),
    bigquery.SchemaField("by_law", "STRING", description="根拠法令"),
    bigquery.SchemaField("full_text", "STRING", description="全文テキスト"),
    bigquery.SchemaField("identified_pattern", "STRING", description="識別されたパターン"),
    bigquery.SchemaField("parsed", "BOOL", description="パース済みフラグ"),
    bigquery.SchemaField("parsed_at", "TIMESTAMP", description="パース日時"),
    bigquery.SchemaField("parse_error", "STRING", description="パースエラーメッセージ"),
    bigquery.SchemaField("created_at", "TIMESTAMP", description="レコード作成日時"),
    bigquery.SchemaField("updated_at", "TIMESTAMP", description="レコード更新日時"),
]

table_id_layer1 = f"{PROJECT_ID}.{DATASET_ID}.raw_announcements"

try:
    table_layer1 = bigquery.Table(table_id_layer1, schema=raw_announcements_schema)
    table_layer1.description = "国債告示の生データ（Layer1）。v9最終改訂版（修正4）対応。"
    
    table_layer1 = client.create_table(table_layer1)
    print(f"✓ 成功: テーブル {table_layer1.table_id} を作成しました")
    print(f"  フィールド数: {len(table_layer1.schema)}")
    print()
except Exception as e:
    if 'Already Exists' in str(e):
        print(f"⊘ スキップ: テーブル {table_id_layer1} は既に存在します")
        print()
    else:
        print(f"✗ エラー: {e}")
        print()

# Layer2テーブル: bond_issuances
print("Layer2テーブル作成中: bond_issuances")
print("-" * 80)

bond_issuances_schema = [
    # 基本フィールド
    bigquery.SchemaField("announcement_id", "STRING", mode="REQUIRED", description="告示ID（Layer1と結合）"),
    bigquery.SchemaField("bond_name", "STRING", description="銘柄名"),
    bigquery.SchemaField("issue_amount", "INT64", description="発行金額（円）"),
    bigquery.SchemaField("legal_basis", "STRING", description="発行根拠法令"),
    bigquery.SchemaField("redemption_per_100", "INT64", description="額面100円あたりの償還金額"),
    bigquery.SchemaField("maturity_date_text", "STRING", description="償還期限（テキスト形式）"),
    
    # v9新フィールド: 法令情報の包括的抽出
    bigquery.SchemaField("legal_basis_extracted", "STRING", description="抽出された法令参照（元の形式）"),
    bigquery.SchemaField("legal_basis_normalized", "STRING", description="正規化された法令キー"),
    bigquery.SchemaField("legal_basis_source", "STRING", description="法令情報の抽出元（by_law/full_text/bond_name/none）"),
    
    # v9新フィールド: 国債種別の自動分類
    bigquery.SchemaField("bond_category", "STRING", description="国債種別（建設国債、借換債、財投債等）"),
    bigquery.SchemaField("mof_category", "STRING", description="財務省統計上の分類"),
    
    # v9新フィールド: データ品質
    bigquery.SchemaField("data_quality_score", "INT64", description="データ品質スコア（0-100）"),
    
    # v9新フィールド: 二重計上防止
    bigquery.SchemaField("is_summary_record", "BOOL", description="総額レコードか（集計対象外）"),
    bigquery.SchemaField("is_detail_record", "BOOL", description="詳細レコードか（集計対象）"),
    
    # メタデータ
    bigquery.SchemaField("created_at", "TIMESTAMP", description="レコード作成日時"),
    bigquery.SchemaField("updated_at", "TIMESTAMP", description="レコード更新日時"),
]

table_id_layer2 = f"{PROJECT_ID}.{DATASET_ID}.bond_issuances"

try:
    table_layer2 = bigquery.Table(table_id_layer2, schema=bond_issuances_schema)
    table_layer2.description = "国債発行情報（Layer2）。v9最終改訂版（修正4）対応。"
    
    # パーティショニング設定
    table_layer2.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="created_at"
    )
    
    table_layer2 = client.create_table(table_layer2)
    print(f"✓ 成功: テーブル {table_layer2.table_id} を作成しました")
    print(f"  フィールド数: {len(table_layer2.schema)}")
    print(f"  v9新フィールド: 8件（法令抽出、国債分類、品質スコア、二重計上防止）")
    print()
except Exception as e:
    if 'Already Exists' in str(e):
        print(f"⊘ スキップ: テーブル {table_id_layer2} は既に存在します")
        print()
    else:
        print(f"✗ エラー: {e}")
        print()

# 作成されたテーブルの確認
print("=" * 80)
print("作成されたテーブルの確認")
print("=" * 80)
print()

try:
    tables = list(client.list_tables(DATASET_ID))
    
    if tables:
        print(f"データセット '{DATASET_ID}' 内のテーブル: {len(tables)}件")
        print()
        for table in tables:
            table_ref = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{table.table_id}")
            print(f"  テーブル: {table.table_id}")
            print(f"    フィールド数: {len(table_ref.schema)}")
            print(f"    行数: {table_ref.num_rows:,}")
            print(f"    説明: {table_ref.description}")
            print()
    else:
        print(f"⚠ データセット '{DATASET_ID}' にテーブルがありません")
        print()
        
except Exception as e:
    print(f"✗ エラー: {e}")
    print()

print("=" * 80)
print("テーブル作成完了")
print("=" * 80)
print()
print("次のステップ:")
print("  1. v9パーサーのPROJECT_IDとDATASET_IDを更新")
print(f"     PROJECT_ID = '{PROJECT_ID}'")
print(f"     DATASET_ID = '{DATASET_ID}'")
print("  2. テスト実行（10ファイル）")
print("  3. 全件処理（179ファイル）")
print()