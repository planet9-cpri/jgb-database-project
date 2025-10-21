# tests/test_kanpo_parser.py
"""
KanpoParserの動作テスト
"""# tests/test_kanpo_parser.py
"""
KanpoParserの動作テスト
"""

import sys
from pathlib import Path

# プロジェクトルートをPythonパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from parsers.kanpo_parser import KanpoParser

def test_single_file():
    """単一ファイルのパーステスト"""
    
    # テストファイルのパス
    test_file = Path(r"G:\マイドライブ\JGBデータ\2023\20230322_財務省告示第123号.txt")
    
    # ファイルが存在するか確認
    if not test_file.exists():
        print(f"❌ ファイルが見つかりません: {test_file}")
        # 代わりに最初のファイルを使用
        data_dir = Path(r"G:\マイドライブ\JGBデータ\2023")
        files = list(data_dir.glob("*.txt"))
        if files:
            test_file = files[0]
            print(f"📄 代わりに最初のファイルを使用: {test_file.name}")
        else:
            print("❌ 2023年度のファイルが見つかりません")
            return None
    
    print(f"📄 テストファイル: {test_file.name}")
    print("="*60)
    
    # パーサーを実行
    parser = KanpoParser()
    result = parser.parse_file(test_file)
    
    # 結果を表示
    if result:
        print("✅ パース成功")
        print(f"告示番号: {result.get('announcement_number')}")
        print(f"告示日: {result.get('announcement_date')}")
        print(f"\n本文（最初の200文字）:")
        content = result.get('content', '')
        print(content[:200])
        print(f"\n本文の長さ: {len(content)}文字")
    else:
        print("❌ パース失敗")
    
    return result

if __name__ == "__main__":
    test_single_file()

from pathlib import Path
from parsers.kanpo_parser import KanpoParser

def test_single_file():
    """単一ファイルのパーステスト"""
    
    # テストファイルのパス
    test_file = Path(r"G:\マイドライブ\JGBデータ\2023\20230322_財務省告示第123号.txt")
    
    print(f"📄 テストファイル: {test_file.name}")
    print("="*60)
    
    # パーサーを実行
    parser = KanpoParser()
    result = parser.parse_file(test_file)
    
    # 結果を表示
    if result:
        print("✅ パース成功")
        print(f"告示番号: {result.get('announcement_number')}")
        print(f"告示日: {result.get('announcement_date')}")
        print(f"本文（最初の200文字）:")
        print(result.get('content', '')[:200])
    else:
        print("❌ パース失敗")
    
    return result

if __name__ == "__main__":
    test_single_file()