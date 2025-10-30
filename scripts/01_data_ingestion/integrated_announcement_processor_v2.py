"""
統合告示処理スクリプト v2 (Phase 3 - 修正版)

修正内容:
1. extract() → extract_issues() に修正
2. フィールドマッピングを正しく修正
3. 法的根拠の扱いを改善
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict
import uuid

# プロジェクトルートのparsersフォルダをパスに追加
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / 'parsers'))

# legal_basis_extractor
try:
    from legal_basis_extractor_v3_clean import extract_legal_bases_structured
    print("✅ legal_basis_extractor_v3_clean を使用")
except ImportError:
    from legal_basis_extractor_v3 import extract_legal_bases_structured
    print("⚠️ legal_basis_extractor_v3 (デバッグ版) を使用")

# IssueExtractor
try:
    from issue_extractor import IssueExtractor
    HAS_ISSUE_EXTRACTOR = True
    print("✅ IssueExtractor を使用")
except ImportError:
    HAS_ISSUE_EXTRACTOR = False
    print("⚠️ IssueExtractor が見つかりません")

# BigQuery クライアント
from google.cloud import bigquery
from google.oauth2 import service_account


# ========================================
# 設定
# ========================================

KANPO_DIR = r"G:\マイドライブ\JGBデータ\2023"
PROJECT_ID = "jgb2023"
DATASET_ID = "20251026"
TABLE_ID = "bond_issuances"
CREDENTIALS_PATH = r"C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json"
OUTPUT_DIR = project_root / 'output' / 'phase3'
LOG_DIR = project_root / 'logs'


# ========================================
# 統合処理クラス v2
# ========================================

class IntegratedAnnouncementProcessorV2:
    """発行情報と法的根拠を統合処理するクラス（修正版）"""
    
    def __init__(self):
        self.has_issue_extractor = HAS_ISSUE_EXTRACTOR
        self.bq_client = None
    
    def init_bigquery_client(self):
        """BigQuery クライアントを初期化"""
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_PATH,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        
        self.bq_client = bigquery.Client(
            credentials=credentials,
            project=PROJECT_ID,
        )
    
    def process_single_file(self, file_path: str) -> Dict:
        """1つの告示ファイルを処理"""
        result = {
            'file_name': os.path.basename(file_path),
            'success': False,
            'integrated_data': [],
            'error': None
        }
        
        try:
            # ファイル読み込み
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Step 1: 法的根拠の抽出
            legal_bases = extract_legal_bases_structured(content)
            
            # Step 2: 発行情報の抽出（修正）
            if self.has_issue_extractor:
                try:
                    issue_extractor = IssueExtractor(content)
                    # 修正: extract() → extract_issues()
                    issues = issue_extractor.extract_issues()
                    
                    if not isinstance(issues, list):
                        issues = [issues] if issues else []
                except Exception as e:
                    print(f"    ⚠️ IssueExtractor エラー: {e}")
                    issues = self._extract_basic_info(file_path, content)
            else:
                issues = self._extract_basic_info(file_path, content)
            
            # Step 3: データの統合（改善版）
            integrated = self._integrate_data(file_path, issues, legal_bases)
            
            result['integrated_data'] = integrated
            result['success'] = True
            
        except Exception as e:
            result['error'] = str(e)
            import traceback
            result['error_detail'] = traceback.format_exc()
        
        return result
    
    def _extract_basic_info(self, file_path: str, content: str) -> List[Dict]:
        """基本情報の抽出（フォールバック）"""
        file_name = os.path.basename(file_path)
        
        return [{
            'source_file': file_name,
            'file_path': file_path,
        }]
    
    def _integrate_data(self, file_path: str, issues: List[Dict], legal_bases: List[Dict]) -> List[Dict]:
        """
        発行情報と法的根拠を統合（改善版）
        
        重要な変更:
        - 発行情報ごとに1行（法的根拠は配列ではなく最初の1つを使用）
        - または、法的根拠が複数ある場合は最初の1つを代表として使用
        """
        integrated = []
        file_name = os.path.basename(file_path)
        
        # 法的根拠を取得（最初の1つを使用）
        primary_legal_basis = legal_bases[0] if legal_bases else None
        
        # 各発行情報について
        for issue in issues:
            # ユニークなIDを生成
            issuance_id = str(uuid.uuid4())
            
            # IssueExtractorのデータ構造に基づいてマッピング
            row = {
                'issuance_id': issuance_id,
                'source_file': file_name,
                # 発行情報（修正: 正しいキー名を使用）
                'bond_name': issue.get('name'),
                'bond_type': issue.get('bond_type'),
                'series_number': issue.get('series_number'),
                'interest_rate': issue.get('rate'),
                'maturity_date': issue.get('maturity_date'),
                'issue_amount': issue.get('amount'),  # ← 修正: 'amount' を使用
            }
            
            # 法的根拠を追加
            if primary_legal_basis:
                row['legal_basis'] = primary_legal_basis['basis']  # 短縮版
                row['legal_basis_full'] = primary_legal_basis['full']  # 完全版
                row['category'] = primary_legal_basis['category']
                row['sub_category'] = primary_legal_basis['sub_category']
            
            integrated.append(row)
        
        return integrated
    
    def process_all_files(self, file_limit: int = None) -> Dict:
        """全告示ファイルを処理"""
        print("=" * 80)
        print("統合告示処理スクリプト v2（修正版）")
        print("=" * 80)
        
        # ディレクトリの存在確認
        if not os.path.exists(KANPO_DIR):
            print(f"❌ エラー: 官報ディレクトリが見つかりません")
            print(f"   パス: {KANPO_DIR}")
            return None
        
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        
        print(f"官報ディレクトリ: {KANPO_DIR}")
        print(f"BigQuery: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        print("")
        
        # 告示ファイルの一覧取得
        files = sorted([f for f in os.listdir(KANPO_DIR) if f.endswith('.txt')])
        
        if file_limit:
            files = files[:file_limit]
            print(f"⚠️ テストモード: 最初の{file_limit}件のみ処理")
        
        print(f"告示ファイル数: {len(files)}件")
        print("=" * 80)
        
        # 各ファイルを処理
        results = []
        success_count = 0
        error_count = 0
        total_integrated = 0
        
        for i, file_name in enumerate(files, 1):
            file_path = os.path.join(KANPO_DIR, file_name)
            
            print(f"\n[{i}/{len(files)}] 処理中: {file_name}")
            
            result = self.process_single_file(file_path)
            results.append(result)
            
            if result['success']:
                success_count += 1
                count = len(result['integrated_data'])
                total_integrated += count
                print(f"  ✅ 成功 - 統合データ: {count}件")
                
                # 最初の1件の詳細を表示
                if count > 0:
                    first = result['integrated_data'][0]
                    if 'legal_basis' in first and first['legal_basis']:
                        print(f"      法的根拠: {first['legal_basis']}")
                    if 'issue_amount' in first and first['issue_amount']:
                        amount_oku = first['issue_amount'] / 100000000
                        print(f"      発行額: {amount_oku:.0f}億円")
            else:
                error_count += 1
                print(f"  ❌ エラー: {result['error']}")
        
        # 結果の集計
        print("\n" + "=" * 80)
        print("【処理結果】")
        print("=" * 80)
        print(f"総ファイル数: {len(files)}件")
        print(f"成功: {success_count}件")
        print(f"失敗: {error_count}件")
        print(f"統合データ総数: {total_integrated}件")
        
        return {
            'timestamp': datetime.now().isoformat(),
            'kanpo_dir': KANPO_DIR,
            'total_files': len(files),
            'success_count': success_count,
            'error_count': error_count,
            'total_integrated': total_integrated,
            'results': results
        }
    
    def upload_to_bigquery(self, process_result: Dict):
        """処理結果をBigQueryに投入（修正版）"""
        print("\n" + "=" * 80)
        print("BigQuery データ投入 v2")
        print("=" * 80)
        
        if not self.bq_client:
            print("BigQuery クライアント初期化中...")
            self.init_bigquery_client()
            print("✅ 初期化完了")
        
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        print(f"投入先テーブル: {table_ref}")
        print("")
        
        # データの準備（修正版）
        rows_to_insert = []
        
        for result in process_result['results']:
            if not result['success']:
                continue
            
            for data in result['integrated_data']:
                # BigQueryスキーマに合わせて変換
                row = {
                    'issuance_id': data.get('issuance_id'),
                    'legal_basis': data.get('legal_basis'),
                    'legal_basis_full': data.get('legal_basis_full'),
                    'category': data.get('category'),
                    'sub_category': data.get('sub_category'),
                    # 発行情報を追加（修正版）
                    'issue_amount': data.get('issue_amount'),
                    'interest_rate': data.get('interest_rate'),
                    # 修正: DATE型用に日付のみを送信
                    'maturity_date': data.get('maturity_date').strftime('%Y-%m-%d') if data.get('maturity_date') else None,
                    'series_number': str(data.get('series_number')) if data.get('series_number') else None,
                }
                
                # None値を除外（issuance_id以外）
                row = {k: v for k, v in row.items() if k == 'issuance_id' or v is not None}
                
                if row.get('issuance_id'):
                    rows_to_insert.append(row)
        
        print(f"投入予定行数: {len(rows_to_insert)}")
        
        if len(rows_to_insert) == 0:
            print("⚠️ 投入するデータがありません")
            return
        
        # プレビュー
        print("\n【データプレビュー（最初の3行）】")
        for i, row in enumerate(rows_to_insert[:3], 1):
            print(f"{i}. ID: {row.get('issuance_id')[:8]}...")
            print(f"   法的根拠: {row.get('legal_basis')}")
            if row.get('issue_amount'):
                print(f"   発行額: {row.get('issue_amount')/100000000:.0f}億円")
        print("")
        
        # 確認
        response = input("BigQueryに投入しますか？ (yes/no): ")
        if response.lower() != 'yes':
            print("キャンセルしました")
            return
        
        # BigQueryに投入
        print("\nBigQueryに投入中...")
        
        try:
            errors = self.bq_client.insert_rows_json(table_ref, rows_to_insert)
            
            if errors:
                print("❌ エラーが発生しました:")
                for error in errors:
                    print(f"  {error}")
            else:
                print(f"✅ {len(rows_to_insert)}行を投入しました")
        
        except Exception as e:
            print(f"❌ 例外が発生しました: {e}")
            import traceback
            traceback.print_exc()
        
        print("=" * 80)


# ========================================
# メイン処理
# ========================================

def main():
    """メイン処理"""
    processor = IntegratedAnnouncementProcessorV2()
    
    print("処理モードを選択してください:")
    print("1. テストモード（最初の5件のみ処理）")
    print("2. 本番モード（全件処理）")
    
    choice = input("選択 (1/2): ").strip()
    
    if choice == '1':
        file_limit = 5
    elif choice == '2':
        file_limit = None
    else:
        print("無効な選択です")
        return
    
    # 全ファイルを処理
    result = processor.process_all_files(file_limit=file_limit)
    
    if result is None:
        return
    
    # 結果をJSON形式で保存
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_file = OUTPUT_DIR / f'integrated_result_v2_{timestamp}.json'
    
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    
    print(f"\n結果を保存しました: {json_file}")
    
    # BigQueryに投入するか確認
    print("\n" + "=" * 80)
    response = input("BigQueryにデータを投入しますか？ (yes/no): ")
    
    if response.lower() == 'yes':
        processor.upload_to_bigquery(result)


if __name__ == "__main__":
    main()