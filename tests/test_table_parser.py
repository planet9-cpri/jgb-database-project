# tests/test_table_parser.py
"""
TableParserの動作テスト
"""

import sys
from pathlib import Path

# プロジェクトルートをPythonパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from parsers.table_parser import TableParser

def test_5col_format():
    """5列フォーマットのテスト"""
    print("\n" + "="*60)
    print("🧪 5列フォーマットのテスト")
    print("="*60)
    
    test_file = Path("tests/fixtures/利付債券（別表5列）.txt")
    
    if not test_file.exists():
        print(f"❌ テストファイルが見つかりません: {test_file}")
        return
    
    with open(test_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    parser = TableParser()
    bonds = parser.parse_table(content)
    
    print(f"📊 抽出された銘柄数: {len(bonds)}")
    print("\n最初の5銘柄:")
    print("-"*60)
    
    for bond in bonds[:5]:
        print(f"{bond.sub_index}. {bond.bond_name}")
        print(f"   利率: {bond.interest_rate}%")
        print(f"   償還期限: {bond.maturity_date}")
        print(f"   発行根拠: {bond.legal_basis}")
        print(f"   発行額: {bond.face_value_individual:,.0f}円")
        print()

def test_4col_format():
    """4列フォーマットのテスト"""
    print("\n" + "="*60)
    print("🧪 4列フォーマットのテスト")
    print("="*60)
    
    test_file = Path("tests/fixtures/利付債券（別表4列）.txt")
    
    if not test_file.exists():
        print(f"❌ テストファイルが見つかりません: {test_file}")
        return
    
    with open(test_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    parser = TableParser()
    bonds = parser.parse_table(content)
    
    print(f"📊 抽出された銘柄数: {len(bonds)}")
    print("\n最初の5銘柄:")
    print("-"*60)
    
    for bond in bonds[:5]:
        print(f"{bond.sub_index}. {bond.bond_name}")
        print(f"   利率: {bond.interest_rate}%")
        print(f"   償還期限: {bond.maturity_date}")
        print(f"   発行根拠: {bond.legal_basis}")
        print(f"   発行額: {bond.face_value_individual:,.0f}円")
        print()

if __name__ == "__main__":
    test_5col_format()
    test_4col_format()
    
    print("\n" + "="*60)
    print("✅ テスト完了")
    print("="*60)