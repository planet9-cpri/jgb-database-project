"""
v9最終改訂版（修正4）パーサーのテスト実行スクリプト

プロジェクト情報:
- プロジェクトID: jgb2023
- データセットID: 20251029
- ロケーション: asia-northeast1
- 官報データ: G:/マイドライブ/JGBデータ/2023/
- 認証ファイル: C:/Users/sonke/secrets/jgb2023-f8c9b849ae2d.json
"""

import os
import sys
from pathlib import Path
from google.cloud import bigquery

# パーサーをインポート
# ※ universal_announcement_parser_v9_final_rev4.py と同じディレクトリに配置してください
try:
    from universal_announcement_parser_v9_final_rev4 import UniversalAnnouncementParser
except ImportError:
    print("エラー: universal_announcement_parser_v9_final_rev4.py が見つかりません")
    print("パーサーファイルを同じディレクトリに配置してください")
    sys.exit(1)

# 設定
PROJECT_ID = 'jgb2023'
DATASET_ID = '20251029'
LOCATION = 'asia-northeast1'
CREDENTIALS_PATH = r'C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json'
DATA_DIR = r'G:\マイドライブ\JGBデータ\2023'

# 認証情報の設定
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS_PATH

print("=" * 80)
print("v9最終改訂版（修正4）パーサー テスト実行")
print("=" * 80)
print(f"プロジェクトID: {PROJECT_ID}")
print(f"データセットID: {DATASET_ID}")
print(f"データディレクトリ: {DATA_DIR}")
print("=" * 80)
print()

# パーサー初期化
parser = UniversalAnnouncementParser(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    credentials_path=CREDENTIALS_PATH
)

# BigQueryクライアント（検証用）
client = bigquery.Client(project=PROJECT_ID, location=LOCATION)

# ステップ1: データディレクトリの確認
print("ステップ1: データディレクトリの確認")
print("-" * 80)

data_path = Path(DATA_DIR)
if not data_path.exists():
    print(f"✗ エラー: データディレクトリが見つかりません: {DATA_DIR}")
    sys.exit(1)

# txtファイルを取得
txt_files = list(data_path.glob("*.txt"))
print(f"✓ データディレクトリが見つかりました")
print(f"  .txtファイル数: {len(txt_files)}件")
print()

if len(txt_files) == 0:
    print("✗ エラー: .txtファイルが見つかりません")
    sys.exit(1)

# ステップ2: テストファイルの選択
print("ステップ2: テストファイルの選択")
print("-" * 80)

# 最初の10ファイルを選択
test_files = txt_files[:10]
print(f"テスト対象: {len(test_files)}件")
print()

for i, file_path in enumerate(test_files, 1):
    print(f"  {i}. {file_path.name}")
print()

# ステップ3: Layer1へのデータ投入（raw_announcements）
print("ステップ3: Layer1へのデータ投入")
print("-" * 80)

table_id_layer1 = f"{PROJECT_ID}.{DATASET_ID}.raw_announcements"

# 既存のannouncement_idを取得
existing_ids = set()
try:
    query = f"SELECT announcement_id FROM `{table_id_layer1}`"
    results = client.query(query, location=LOCATION).result()
    existing_ids = {row.announcement_id for row in results}
    print(f"既存レコード数: {len(existing_ids)}件")
except Exception as e:
    print(f"⚠ 既存レコード確認エラー（初回実行の場合は正常）: {e}")

print()

# テストファイルのデータを準備
rows_to_insert = []
file_list_for_parser = []

for file_path in test_files:
    # announcement_idを生成（ファイル名から）
    announcement_id = file_path.stem  # 拡張子なしのファイル名
    
    # 既に存在する場合はスキップ
    if announcement_id in existing_ids:
        print(f"⊘ スキップ: {announcement_id}（既存）")
        continue
    
    # ファイル内容を読み込み
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            full_text = f.read()
        
        # Layer1用のレコードを作成
        row = {
            'announcement_id': announcement_id,
            'file_path': str(file_path),
            'file_name': file_path.name,
            'full_text': full_text,
            'parsed': False,
        }
        rows_to_insert.append(row)
        
        # パーサー用のリストにも追加
        file_list_for_parser.append((str(file_path), {'announcement_id': announcement_id}))
        
        print(f"✓ 準備完了: {announcement_id}")
        
    except Exception as e:
        print(f"✗ エラー: {file_path.name} - {e}")

print()

# Layer1へ投入
if rows_to_insert:
    print(f"Layer1へ投入中: {len(rows_to_insert)}件")
    try:
        errors = client.insert_rows_json(table_id_layer1, rows_to_insert)
        if errors:
            print(f"✗ エラー: {errors}")
        else:
            print(f"✓ 成功: {len(rows_to_insert)}件を投入しました")
    except Exception as e:
        print(f"✗ エラー: {e}")
else:
    print("投入するデータがありません（全て既存）")

print()

# ステップ4: パーサー実行
print("ステップ4: パーサー実行（Layer2投入）")
print("-" * 80)

if file_list_for_parser:
    results = parser.batch_process(file_list_for_parser)
    
    print()
    print("=" * 80)
    print("パーサー実行結果")
    print("=" * 80)
    print(f"総ファイル数: {results['total']}件")
    print(f"成功: {results['success']}件")
    print(f"失敗: {results['failure']}件")
    print("=" * 80)
    print()
else:
    print("パーサー実行対象がありません")
    print()

# ステップ5: 結果の検証
print("ステップ5: 結果の検証")
print("-" * 80)

# Layer2のデータを確認
table_id_layer2 = f"{PROJECT_ID}.{DATASET_ID}.bond_issuances"

try:
    # レコード数の確認
    count_query = f"SELECT COUNT(*) as cnt FROM `{table_id_layer2}`"
    result = list(client.query(count_query, location=LOCATION).result())[0]
    total_records = result.cnt
    
    print(f"Layer2レコード数: {total_records}件")
    print()
    
    # v9新フィールドのサンプルを確認
    sample_query = f"""
    SELECT 
        bond_name,
        issue_amount,
        legal_basis,
        legal_basis_normalized,
        legal_basis_source,
        bond_category,
        mof_category,
        data_quality_score,
        is_summary_record,
        is_detail_record
    FROM `{table_id_layer2}`
    LIMIT 5
    """
    
    print("サンプルレコード（最初の5件）:")
    print("-" * 80)
    
    results = client.query(sample_query, location=LOCATION).result()
    for i, row in enumerate(results, 1):
        print(f"{i}. 銘柄名: {row.bond_name}")
        print(f"   発行金額: {row.issue_amount:,}円" if row.issue_amount else "   発行金額: None")
        print(f"   法令（元）: {row.legal_basis}")
        print(f"   法令（正規化）: {row.legal_basis_normalized}")
        print(f"   法令抽出元: {row.legal_basis_source}")
        print(f"   国債種別: {row.bond_category}")
        print(f"   財務省分類: {row.mof_category}")
        print(f"   品質スコア: {row.data_quality_score}")
        print(f"   総額レコード: {row.is_summary_record}")
        print(f"   詳細レコード: {row.is_detail_record}")
        print()
    
    # 統計情報
    stats_query = f"""
    SELECT
        legal_basis_source,
        COUNT(*) as record_count,
        AVG(data_quality_score) as avg_quality
    FROM `{table_id_layer2}`
    GROUP BY legal_basis_source
    ORDER BY record_count DESC
    """
    
    print("統計情報: 法令抽出元別")
    print("-" * 80)
    results = client.query(stats_query, location=LOCATION).result()
    for row in results:
        avg_quality = row.avg_quality if row.avg_quality else 0
        print(f"  {row.legal_basis_source:15s}: {row.record_count:3d}件（平均品質: {avg_quality:.1f}点）")
    print()
    
    # 国債種別の分布
    category_query = f"""
    SELECT
        bond_category,
        mof_category,
        COUNT(*) as record_count,
        SUM(issue_amount) / 100000000 as total_billion_yen
    FROM `{table_id_layer2}`
    WHERE is_detail_record = TRUE
    GROUP BY bond_category, mof_category
    ORDER BY total_billion_yen DESC
    """
    
    print("統計情報: 国債種別別（詳細レコードのみ）")
    print("-" * 80)
    results = client.query(category_query, location=LOCATION).result()
    total_amount = 0
    for row in results:
        amount = row.total_billion_yen if row.total_billion_yen else 0
        total_amount += amount
        print(f"  {row.bond_category:30s} ({row.mof_category:15s}): {row.record_count:3d}件, {amount:10,.0f}億円")
    print(f"  {'合計':30s} {'':15s}: {'':<3s}  {total_amount:10,.0f}億円")
    print()
    
except Exception as e:
    print(f"✗ エラー: {e}")
    print()

print("=" * 80)
print("テスト実行完了")
print("=" * 80)
print()
print("次のステップ:")
print("  1. 結果を確認し、v9新機能が正しく動作しているか検証")
print("  2. 問題なければ、全179ファイルを処理")
print("  3. 財務省統計との照合")
print()