# tests/test_additional_patterns2.py
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
    
    # KanpoParserでパース
    print("\n🔍 KanpoParserでパース:")
    print("-"*60)
    
    kanpo_parser = KanpoParser()
    result = kanpo_parser.parse_file(str(filepath))
    
    if result:
        print(f"✅ パース成功")
        print(f"  告示番号: {result.get('announcement_number')}")
        print(f"  告示日: {result.get('announcement_date')}")
        print(f"  別表数: {len(result.get('tables', []))}")
        
        # 単一銘柄の抽出
        print("\n📋 単一銘柄の抽出:")
        table_parser = TableParser()
        bond = table_parser.extract_bond_info_from_single(content)
        
        if bond:
            print(f"  ✅ 単一銘柄抽出成功:")
            print(f"     銘柄名: {bond.bond_name}")
            print(f"     債券種別: {bond.bond_type}")
            print(f"     利率: {bond.interest_rate}%")
            print(f"     償還期限: {bond.maturity_date}")
            print(f"     発行根拠: {bond.legal_basis[:80]}...")
            print(f"     発行額: {bond.face_value_individual:,.0f}円")
        else:
            print(f"  ⚠️  単一銘柄の抽出に失敗")
    else:
        print(f"❌ パース失敗")

def main():
    print("\n" + "="*60)
    print("🧪 追加パターン2のテスト")
    print("="*60)
    
    # テストファイルのパス
    test_files = [
        (Path("tests/fixtures/利付債券（別表なし）.txt"), "利付債券（別表なし・募集取扱）"),
        (Path("tests/fixtures/利付債券（物価連動）.txt"), "利付債券（物価連動）"),
        (Path("tests/fixtures/利付債券（GX債権）.txt"), "利付債券（GX債券・クライメートトランジション）"),
    ]
    
    for filepath, pattern_name in test_files:
        test_pattern(filepath, pattern_name)
    
    print("\n" + "="*60)
    print("✅ 全パターンのテスト完了")
    print("="*60)

if __name__ == "__main__":
    main()