# tests/debug_table_parser.py
"""
TableParserのデバッグスクリプト
"""

import sys
from pathlib import Path
import re

# プロジェクトルートをPythonパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def debug_table_structure(filepath: Path):
    """別表の構造をデバッグ"""
    
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
    
    # 1. 別表の開始位置を検索
    print("\n🔍 ステップ1: 別表の開始位置を検索")
    pattern_table_start = r'（別\s*表）'
    table_match = re.search(pattern_table_start, content)
    
    if table_match:
        print(f"✅ 別表が見つかりました（位置: {table_match.start()}）")
        print(f"マッチ文字列: '{table_match.group(0)}'")
        
        # 別表の周辺を表示
        start = table_match.start()
        context = content[max(0, start-50):start+200]
        print(f"\n前後のコンテキスト:")
        print("-"*60)
        print(context)
        print("-"*60)
        
        # 別表部分を抽出
        table_text = content[start:]
        lines = table_text.split('\n')
        
        # 2. ヘッダー行を検索
        print("\n🔍 ステップ2: ヘッダー行を検索")
        header_idx = None
        for i, line in enumerate(lines[:20]):  # 最初の20行を確認
            print(f"行{i}: {line[:80]}")
            if '名称及び記号' in line:
                header_idx = i
                print(f"✅ ヘッダー行が見つかりました（行{i}）")
                break
        
        if header_idx is None:
            print("❌ ヘッダー行が見つかりませんでした")
            return
        
        # 3. フォーマット判定
        print("\n🔍 ステップ3: フォーマット判定")
        header_line = lines[header_idx]
        if '発行の根拠法律' in header_line:
            print("✅ 5列フォーマット")
        else:
            print("✅ 4列フォーマット")
        
        # 4. データ行を検索
        print("\n🔍 ステップ4: データ行を検索（最初の10行）")
        bond_count = 0
        for i, line in enumerate(lines[header_idx + 1:header_idx + 20], start=header_idx + 1):
            if not line.strip() or 'page=' in line:
                continue
            
            print(f"\n行{i}:")
            print(f"  内容: {line[:100]}")
            
            if '利付国庫債券' in line:
                bond_count += 1
                print(f"  ✅ 銘柄行を発見（{bond_count}個目）")
                
                # 各要素を抽出
                bond_pattern = r'利付国庫債券（(\d+)年）（第(\d+)回）'
                bond_match = re.search(bond_pattern, line)
                if bond_match:
                    print(f"    銘柄名: {bond_match.group(0)}")
                
                rate_pattern = r'([\d.]+)％'
                rate_match = re.search(rate_pattern, line)
                if rate_match:
                    print(f"    利率: {rate_match.group(1)}%")
                
                amount_pattern = r'([\d,]+)円'
                amount_match = re.search(amount_pattern, line)
                if amount_match:
                    print(f"    発行額: {amount_match.group(1)}円")
            else:
                print(f"  ⚠️  銘柄行ではない")
        
        print(f"\n📊 合計: {bond_count}銘柄行を発見")
        
    else:
        print("❌ 別表が見つかりませんでした")
        print("\n別表パターンを含む行を検索:")
        for i, line in enumerate(content.splitlines()[:50]):
            if '別表' in line or '別　表' in line:
                print(f"  行{i}: {line}")

if __name__ == "__main__":
    print("\n" + "="*60)
    print("🐛 TableParser デバッグツール")
    print("="*60)
    
    # 5列フォーマット
    test_file_5col = Path("tests/fixtures/利付債券（別表5列）.txt")
    debug_table_structure(test_file_5col)
    
    print("\n" * 3)
    
    # 4列フォーマット
    test_file_4col = Path("tests/fixtures/利付債券（別表4列）.txt")
    debug_table_structure(test_file_4col)