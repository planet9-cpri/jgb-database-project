# tests/test_additional_patterns.py
"""
追加の3パターンの官報ファイルをテスト
"""

import sys
from pathlib import Path

# プロジェクトルートをPythonパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from parsers.kanpo_parser import KanpoParser
from parsers.table_parser import TableParser

def test_pattern(filepath: Path, pattern_name: str):
    """パターンごとのテスト"""
    
    print("\n" + "="*60)
    print(f"📄 パターン: {pattern_name}")
    print(f"ファイル: {filepath.name}")
    print("="*60)
    
    if not filepath.exists():
        print(f"❌ ファイルが見つかりません: {filepath}")
        return
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    print(f"文字数: {len(content)}")
    print(f"行数: {len(content.splitlines())}")
    
    # KanpoParserでパース
    print("\n🔍 KanpoParserでパース:")
    print("-"*60)
    
    kanpo_parser = KanpoParser()
    result = kanpo_parser.parse_file(str(filepath))
    
    if result:
        print(f"✅ パース成功")
        print(f"  告示番号: {result.get('announcement_number')}")
        print(f"  告示日: {result.get('announcement_date')}")
        print(f"  省庁: {result.get('ministry')}")
        print(f"  官報番号: {result.get('kanpo_number')}")
        print(f"  別表数: {len(result.get('tables', []))}")
        
        # 別表があるかチェック
        if result.get('tables'):
            print("\n📊 別表が見つかりました:")
            table_parser = TableParser()
            bonds = table_parser.parse_table(content)
            print(f"  抽出銘柄数: {len(bonds)}")
        else:
            print("\n📋 別表なし - 単一銘柄の抽出を試みます:")
            table_parser = TableParser()
            bond = table_parser.extract_bond_info_from_single(content)
            
            if bond:
                print(f"  ✅ 単一銘柄抽出成功:")
                print(f"     銘柄名: {bond.bond_name}")
                print(f"     利率: {bond.interest_rate}%")
                print(f"     償還期限: {bond.maturity_date}")
                print(f"     発行根拠: {bond.legal_basis}")
                print(f"     発行額: {bond.face_value_individual:,.0f}円")
            else:
                print(f"  ⚠️  単一銘柄の抽出に失敗")
        
        # 本文の一部を表示
        print("\n📄 本文サンプル（最初の300文字）:")
        print("-"*60)
        print(content[:300])
        print("...")
        
    else:
        print(f"❌ パース失敗")

def main():
    print("\n" + "="*60)
    print("🧪 追加パターンのテスト")
    print("="*60)
    
    # テストファイルのパス
    test_files = [
        (Path("tests/fixtures/利付債券（入札発行）.txt"), "利付債券（入札発行）"),
        (Path("tests/fixtures/国庫短期証券（復興債）.txt"), "国庫短期証券（復興債）"),
        (Path("tests/fixtures/利付債券（個人向け）.txt"), "利付債券（個人向け）"),
    ]
    
    for filepath, pattern_name in test_files:
        test_pattern(filepath, pattern_name)
    
    print("\n" + "="*60)
    print("✅ 全パターンのテスト完了")
    print("="*60)

if __name__ == "__main__":
    main()