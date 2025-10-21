# tests/debug_table_parser_v2.py
"""
TableParser デバッグ v2 - ロギング付き
"""

import sys
from pathlib import Path
import logging

# ロギング設定
logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)s - %(message)s'
)

# プロジェクトルートをPythonパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from parsers.table_parser import TableParser

def test_with_logging(filepath: Path):
    """ロギング付きテスト"""
    
    print("="*60)
    print(f"📄 ファイル: {filepath.name}")
    print("="*60)
    
    if not filepath.exists():
        print(f"❌ ファイルが見つかりません")
        return
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    print(f"文字数: {len(content)}")
    print(f"行数: {len(content.splitlines())}")
    print("\n" + "-"*60)
    print("TableParser実行開始")
    print("-"*60 + "\n")
    
    parser = TableParser()
    bonds = parser.parse_table(content)
    
    print("\n" + "-"*60)
    print("TableParser実行完了")
    print("-"*60)
    
    print(f"\n📊 抽出された銘柄数: {len(bonds)}")
    
    if bonds:
        print("\n最初の3銘柄:")
        for bond in bonds[:3]:
            print(f"{bond.sub_index}. {bond.bond_name}")
            print(f"   利率: {bond.interest_rate}%")
            print(f"   償還期限: {bond.maturity_date}")
            print(f"   発行額: {bond.face_value_individual:,.0f}円")
            print()
    else:
        print("⚠️  銘柄が抽出されませんでした")

if __name__ == "__main__":
    print("\n🐛 TableParser デバッグ v2")
    print("="*60)
    
    # 5列フォーマット
    test_file = Path("tests/fixtures/利付債券（別表5列）.txt")
    test_with_logging(test_file)
    
    print("\n" * 2)
    
    # 4列フォーマット
    test_file = Path("tests/fixtures/利付債券（別表4列）.txt")
    test_with_logging(test_file)