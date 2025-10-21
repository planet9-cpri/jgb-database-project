# scripts/load_issuance_data.py
"""
発行データをBigQueryに投入するスクリプト

使用方法:
    python scripts/load_issuance_data.py --year 2023 --limit 10
"""

import sys
from pathlib import Path

# プロジェクトルートをPythonパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import argparse
from google.cloud import bigquery
import pandas as pd
from datetime import datetime

from parsers.kanpo_parser import KanpoParser
# from parsers.table_parser import TableParser  # TableParserはまだ実装していないのでコメントアウト

# 設定
credentials_path = r"C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json"
project_id = "jgb2023"
dataset_id = "20251019"

def load_announcements(client, year: int, limit: int = None):
    """
    官報ファイルから告示データを投入
    
    Args:
        client: BigQueryクライアント
        year: 対象年度
        limit: 処理ファイル数の上限（テスト用）
    """
    
    data_dir = Path(rf"G:\マイドライブ\JGBデータ\{year}")
    files = list(data_dir.glob("*.txt"))
    
    if limit:
        files = files[:limit]
    
    print(f"📂 処理対象: {len(files)}ファイル")
    
    kanpo_parser = KanpoParser()
    # table_parser = TableParser()  # まだ実装していないのでコメントアウト
    
    announcements = []
    
    for i, file in enumerate(files, 1):
        print(f"\n[{i}/{len(files)}] {file.name}")
        
        try:
            # 1. 告示情報を抽出
            announcement = kanpo_parser.parse_file(file)
            if not announcement:
                print("  ⚠️  パース失敗")
                continue
            
            print(f"  ✅ 告示: {announcement.get('announcement_number', 'なし')}")
            print(f"  📄 本文: {len(announcement.get('content', ''))}文字")
            
            # 2. データを蓄積
            announcements.append(announcement)
            
        except Exception as e:
            print(f"  ❌ エラー: {e}")
    
    # 結果サマリー
    print("\n" + "="*60)
    print("📊 パース結果サマリー")
    print("="*60)
    print(f"✅ 成功: {len(announcements)}件")
    print(f"❌ 失敗: {len(files) - len(announcements)}件")
    
    # TableParserが実装されたら、BigQueryへの投入処理を追加

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int, default=2023)
    parser.add_argument('--limit', type=int, default=10, help='テスト用の処理ファイル数上限')
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("🚀 発行データ読み込みテスト")
    print("="*60)
    print(f"対象年度: {args.year}")
    print(f"処理上限: {args.limit}ファイル")
    print()
    
    client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)
    
    load_announcements(client, args.year, args.limit)

if __name__ == "__main__":
    main()