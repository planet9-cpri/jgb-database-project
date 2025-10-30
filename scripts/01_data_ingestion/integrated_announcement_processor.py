"""
統合告示処理スクリプト (Phase 3)

Day 4 の IssueExtractor と v3 の legal_basis_extractor を統合

処理内容:
1. 官報ファイルから発行情報を抽出（IssueExtractor）
2. 同じファイルから法的根拠を抽出（legal_basis_extractor_v3）
3. 両方のデータを統合
4. BigQuery の bond_issuances テーブルに投入

使用方法:
    python integrated_announcement_processor.py
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict

# プロジェクトルートのparsersフォルダをパスに追加
# scripts/01_data_ingestion/ から3階層上がプロジェクトルート
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / 'parsers'))

# デバッグ版を使用（暫定）
try:
    from legal_basis_extractor_v3_clean import extract_legal_bases_structured
    print("✅ legal_basis_extractor_v3_clean を使用")
except ImportError:
    from legal_basis_extractor_v3 import extract_legal_bases_structured
    print("⚠️ legal_basis_extractor_v3 (デバッグ版) を使用")

# IssueExtractor のインポート（Day 4 で作成）
try:
    from issue_extractor import IssueExtractor
    HAS_ISSUE_EXTRACTOR = True
except ImportError:
    # issue_extractor が見つからない場合は、基本的な抽出機能を使用
    HAS_ISSUE_EXTRACTOR = False
    print("⚠️ issue_extractor.py が見つかりません")
    print("   代替の基本抽出機能を使用します")

# BigQuery クライアント
from google.cloud import bigquery
from google.oauth2 import service_account


# ========================================
# 設定
# ========================================

# 官報データディレクトリ
KANPO_DIR = r"G:\マイドライブ\JGBデータ\2023"

# BigQuery 設定
PROJECT_ID = "jgb2023"
DATASET_ID = "20251025"
TABLE_ID = "bond_issuances"

# 認証情報
CREDENTIALS_PATH = r"C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json"

# 出力ディレクトリ
OUTPUT_DIR = project_root / 'output' / 'phase3'
LOG_DIR = project_root / 'logs'


# ========================================
# 統合処理クラス
# ========================================

class IntegratedAnnouncementProcessor:
    """発行情報と法的根拠を統合処理するクラス"""
    
    def __init__(self):
        """初期化"""
        # IssueExtractor は各ファイル処理時にインスタンス化
        # （初期化時にnotice_textが必要なため）
        self.has_issue_extractor = HAS_ISSUE_EXTRACTOR
        
        # BigQuery クライアント
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
        """
        1つの告示ファイルを処理して、発行情報と法的根拠を統合
        
        Returns:
            {
                'file_name': str,
                'success': bool,
                'integrated_data': List[dict],  # 統合されたデータ
                'error': str or None
            }
        """
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
            
            # Step 2: 発行情報の抽出
            if self.has_issue_extractor:
                # Day 4 の IssueExtractor を使用（各ファイルごとにインスタンス化）
                try:
                    issue_extractor = IssueExtractor(content)
                    issues = issue_extractor.extract()
                    # 抽出結果がリストでない場合、リストに変換
                    if not isinstance(issues, list):
                        issues = [issues] if issues else []
                except Exception as e:
                    print(f"    ⚠️ IssueExtractor エラー: {e}")
                    issues = self._extract_basic_info(file_path, content)
            else:
                # 代替: 基本情報のみ抽出
                issues = self._extract_basic_info(file_path, content)
            
            # Step 3: データの統合
            integrated = self._integrate_data(issues, legal_bases)
            
            result['integrated_data'] = integrated
            result['success'] = True
            
        except Exception as e:
            result['error'] = str(e)
            import traceback
            result['error_detail'] = traceback.format_exc()
        
        return result
    
    def _extract_basic_info(self, file_path: str, content: str) -> List[Dict]:
        """
        基本情報の抽出（IssueExtractor がない場合の代替）
        
        ファイル名から基本情報を抽出
        """
        file_name = os.path.basename(file_path)
        
        # ファイル名のパターン: YYYYMMDD_発行日_財務省告示番号.txt
        # 例: 20240220_令和6年3月8日付（財務省第六十三号）.txt
        
        return [{
            'source_file': file_name,
            'file_path': file_path,
            # その他の情報は告示本文から抽出が必要
            # （簡易版では最低限の情報のみ）
        }]
    
    def _integrate_data(self, issues: List[Dict], legal_bases: List[Dict]) -> List[Dict]:
        """
        発行情報と法的根拠を統合
        
        Args:
            issues: 発行情報のリスト
            legal_bases: 法的根拠のリスト
        
        Returns:
            統合されたデータのリスト
        """
        integrated = []
        
        # 各発行情報について
        for issue in issues:
            # 基本情報をコピー
            data = issue.copy()
            
            # 法的根拠を追加
            # 複数の法的根拠がある場合、各法的根拠について行を作成
            if legal_bases:
                for legal_basis in legal_bases:
                    row = data.copy()
                    row['legal_basis'] = legal_basis['full']
                    row['category'] = legal_basis['category']
                    row['sub_category'] = legal_basis['sub_category']
                    row['basis_short'] = legal_basis['basis']
                    integrated.append(row)
            else:
                # 法的根拠が抽出できなかった場合も、発行情報は保持
                data['legal_basis'] = None
                data['category'] = None
                data['sub_category'] = None
                data['basis_short'] = None
                integrated.append(data)
        
        return integrated
    
    def process_all_files(self, file_limit: int = None) -> Dict:
        """
        全告示ファイルを処理
        
        Args:
            file_limit: 処理するファイル数の上限（None = すべて処理）
        
        Returns:
            処理結果の辞書
        """
        print("=" * 80)
        print("統合告示処理スクリプト")
        print("=" * 80)
        
        # ディレクトリの存在確認
        if not os.path.exists(KANPO_DIR):
            print(f"❌ エラー: 官報ディレクトリが見つかりません")
            print(f"   パス: {KANPO_DIR}")
            return None
        
        # 出力ディレクトリの作成
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
                    if 'basis_short' in first and first['basis_short']:
                        print(f"      法的根拠: {first['basis_short']}")
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
        
        # 結果を返す
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
        """
        処理結果をBigQueryに投入
        
        Args:
            process_result: process_all_files() の戻り値
        """
        print("\n" + "=" * 80)
        print("BigQuery データ投入")
        print("=" * 80)
        
        # BigQuery クライアント初期化
        if not self.bq_client:
            print("BigQuery クライアント初期化中...")
            self.init_bigquery_client()
            print("✅ 初期化完了")
        
        # テーブル参照
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        print(f"投入先テーブル: {table_ref}")
        print("")
        
        # データの準備
        rows_to_insert = []
        
        for result in process_result['results']:
            if not result['success']:
                continue
            
            for data in result['integrated_data']:
                # BigQuery の行形式に変換（スキーマに合わせる）
                
                # issuance_id の生成（ユニークなID）
                # ファイル名 + 法的根拠の basis を使用
                file_name = data.get('source_file', result['file_name'])
                basis_short = data.get('basis_short', 'unknown')
                issuance_id = f"{file_name.replace('.txt', '')}_{basis_short}"
                
                row = {
                    'issuance_id': issuance_id,
                    'legal_basis': data.get('basis_short'),  # 短縮版（例: "借換債"）
                    'legal_basis_full': data.get('legal_basis'),  # 完全版（例: "特別会計に関する法律第46条第1項"）
                    'category': data.get('category'),
                    'sub_category': data.get('sub_category'),
                    # IssueExtractor からの情報（将来の拡張用）
                    # 'announcement_id': data.get('announcement_id'),
                    # 'bond_master_id': data.get('bond_master_id'),
                    # 'series_number': data.get('series_number'),
                    # 'issue_amount': data.get('issue_amount'),
                    # 'issuance_date': data.get('issue_date'),
                    # etc.
                }
                
                # None 値を除外
                row = {k: v for k, v in row.items() if v is not None}
                
                if row:  # 空でない場合のみ追加
                    rows_to_insert.append(row)
        
        print(f"投入予定行数: {len(rows_to_insert)}")
        
        if len(rows_to_insert) == 0:
            print("⚠️ 投入するデータがありません")
            return
        
        # 最初の3行をプレビュー
        print("\n【データプレビュー（最初の3行）】")
        for i, row in enumerate(rows_to_insert[:3], 1):
            print(f"{i}. {row}")
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
    # 統合プロセッサーの初期化
    processor = IntegratedAnnouncementProcessor()
    
    # 処理モードの選択
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
    json_file = OUTPUT_DIR / f'integrated_result_{timestamp}.json'
    
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print(f"\n結果を保存しました: {json_file}")
    
    # BigQueryに投入するか確認
    print("\n" + "=" * 80)
    response = input("BigQueryにデータを投入しますか？ (yes/no): ")
    
    if response.lower() == 'yes':
        processor.upload_to_bigquery(result)


if __name__ == "__main__":
    main()