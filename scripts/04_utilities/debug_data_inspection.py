"""
投入予定データの内容を確認するデバッグスクリプト

使用方法:
    python scripts/debug_data_inspection.py
"""

import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from parsers.kanpo_parser import KanpoParser
from parsers.table_parser import TableParser
import re

DATA_DIR = r"G:\マイドライブ\JGBデータ\2023"


def extract_info_from_filename(filename: str) -> dict:
    """ファイル名から告示情報を抽出"""
    
    def convert_kanji_to_number(kanji_str: str) -> str:
        kanji_to_digit = {
            '一': 1, '二': 2, '三': 3, '四': 4, '五': 5,
            '六': 6, '七': 7, '八': 8, '九': 9,
        }
        
        total = 0
        current = 0
        
        i = 0
        while i < len(kanji_str):
            char = kanji_str[i]
            
            if char in kanji_to_digit:
                current = kanji_to_digit[char]
            elif char == '十':
                if current == 0:
                    current = 10
                else:
                    current *= 10
            elif char == '百':
                if current == 0:
                    current = 100
                else:
                    current *= 100
            elif char == '千':
                if current == 0:
                    current = 1000
                else:
                    current *= 1000
            else:
                i += 1
                continue
            
            if i + 1 < len(kanji_str):
                next_char = kanji_str[i + 1]
                if next_char not in ['十', '百', '千', '万']:
                    total += current
                    current = 0
            else:
                total += current
                current = 0
            
            i += 1
        
        if current > 0:
            total += current
        
        return str(total) if total > 0 else kanji_str
    
    info = {}
    
    # 日付を抽出
    date_match = re.match(r'(\d{8})_', filename)
    if date_match:
        date_str = date_match.group(1)
        info['kanpo_date'] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    
    # 告示番号を抽出
    number_match = re.search(r'財務省第(.+?)号', filename)
    if number_match:
        number_str = number_match.group(1)
        number = convert_kanji_to_number(number_str)
        info['announcement_number'] = f"第{number}号"
    
    # 官報発行日を抽出
    gazette_date_match = re.search(r'(令和\d+年\d+月\d+日付)', filename)
    if gazette_date_match:
        info['gazette_issue_number'] = gazette_date_match.group(1)
    
    info['title'] = ''
    info['announcement_type'] = '発行告示'
    
    return info


def inspect_data():
    """データ内容を詳細に確認"""
    print("="*60)
    print("🔍 投入予定データの詳細確認")
    print("="*60)
    
    # パーサーの初期化
    kanpo_parser = KanpoParser()
    table_parser = TableParser()
    
    # テストファイル
    data_path = Path(DATA_DIR)
    file_path = sorted(data_path.glob("*.txt"))[0]
    
    print(f"\n📄 ファイル: {file_path.name}\n")
    
    # 解析
    result = kanpo_parser.parse_file(str(file_path))
    announcement_info = result.get('announcement_info', {})
    
    if not announcement_info.get('kanpo_date'):
        announcement_info = extract_info_from_filename(file_path.name)
    
    # 銘柄情報の抽出
    bond_issuances = []
    single_issuance = table_parser.extract_bond_info_from_single(
        result.get('content', '')
    )
    if single_issuance:
        bond_issuances.append(single_issuance)
    
    # 告示データの作成
    announcement_data = {
        'announcement_id': f"ANN_{announcement_info['kanpo_date'].replace('-', '')}_{announcement_info['announcement_number'].replace('第', '').replace('号', '')}",
        'kanpo_date': announcement_info['kanpo_date'],
        'announcement_number': str(announcement_info['announcement_number']),
        'gazette_issue_number': str(announcement_info.get('gazette_issue_number', '')),
        'announcement_type': announcement_info.get('announcement_type', '発行告示'),
        'title': announcement_info.get('title', ''),
        'source_file': file_path.name,
        'created_at': datetime.utcnow(),
        'updated_at': datetime.utcnow()
    }
    
    print("【告示データ】")
    for key, value in announcement_data.items():
        print(f"  {key:25s}: {value}")
    
    # DataFrameに変換してスキーマを確認
    print("\n【DataFrameスキーマ】")
    df = pd.DataFrame([announcement_data])
    
    # データ型の変換
    df['kanpo_date'] = pd.to_datetime(df['kanpo_date'], errors='coerce')
    df['announcement_number'] = df['announcement_number'].astype(str)
    df['gazette_issue_number'] = df['gazette_issue_number'].fillna('').astype(str)
    df['title'] = df['title'].fillna('').astype(str)
    
    print(df.dtypes)
    
    print("\n【データ内容】")
    print(df.to_string())
    
    print("\n【データサイズ】")
    print(f"  行数: {len(df)}")
    print(f"  列数: {len(df.columns)}")
    print(f"  メモリ使用量: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")
    
    # NULL値の確認
    print("\n【NULL値の確認】")
    null_counts = df.isnull().sum()
    for col, count in null_counts.items():
        if count > 0:
            print(f"  {col}: {count} 個のNULL")
    if null_counts.sum() == 0:
        print("  NULL値なし ✅")
    
    print("\n" + "="*60)
    print("✅ データ検証完了")
    print("="*60)


if __name__ == "__main__":
    inspect_data()