# tests/debug_table_parser_v3.py
"""
TableParser デバッグ v3 - 強制print版
"""

import sys
from pathlib import Path
import re

# プロジェクトルートをPythonパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def debug_parse_table(filepath: Path):
    """parse_table() を手動で再現してデバッグ"""
    
    print("="*60)
    print(f"📄 ファイル: {filepath.name}")
    print("="*60)
    
    if not filepath.exists():
        print(f"❌ ファイルが見つかりません")
        return
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    print(f"✅ ファイル読み込み完了: {len(content)}文字")
    
    # ステップ1: 別表の開始位置を検索
    print("\n🔍 ステップ1: 別表の開始位置を検索")
    pattern_table_start = r'（別\s*表）'
    table_match = re.search(pattern_table_start, content)
    
    if not table_match:
        print("❌ 別表が見つかりませんでした → 早期リターン")
        return
    
    print(f"✅ 別表が見つかりました（位置: {table_match.start()}）")
    
    # ステップ2: 別表部分を抽出
    print("\n🔍 ステップ2: 別表部分を抽出")
    table_start = table_match.start()
    table_text = content[table_start:]
    lines = table_text.split('\n')
    print(f"✅ 別表の行数: {len(lines)}")
    
    # ステップ3: ヘッダー行を探す
    print("\n🔍 ステップ3: ヘッダー行を探す")
    header_idx = None
    for i, line in enumerate(lines[:20]):
        if '名称及び記号' in line:
            header_idx = i
            print(f"✅ ヘッダー行が見つかりました（行{i}）: '{line}'")
            break
    
    if header_idx is None:
        print("❌ ヘッダー行が見つかりませんでした → 早期リターン")
        return
    
    # ステップ4: フォーマット判定
    print("\n🔍 ステップ4: フォーマット判定")
    header_lines = lines[header_idx:header_idx+10]
    text = '\n'.join(header_lines)
    
    if '発行の根拠法律' in text:
        format_type = 5
        print(f"✅ 5列フォーマット")
    else:
        format_type = 4
        print(f"✅ 4列フォーマット")
    
    # ステップ5: 共通発行根拠の抽出（4列の場合）
    common_legal_basis = None
    if format_type == 4:
        print("\n🔍 ステップ5: 共通発行根拠の抽出")
        pattern = r'２\s*発行の根拠法律及びその条項\s+(.+?)(?:\n|３)'
        match = re.search(pattern, content)
        if match:
            common_legal_basis = match.group(1).strip()
            print(f"✅ 共通発行根拠: {common_legal_basis}")
        else:
            print("⚠️  共通発行根拠が見つかりませんでした")
    
    # ステップ6: データ行の解析
    print("\n🔍 ステップ6: データ行の解析")
    print(f"開始位置: header_idx={header_idx}, format_type={format_type}")
    
    bonds = []
    sub_index = 0
    i = header_idx + 1
    
    print(f"\n最初の20行をチェック:")
    check_count = 0
    
    while i < len(lines) and check_count < 20:
        line = lines[i].strip()
        check_count += 1
        
        # デバッグ出力
        print(f"\n行{i}: '{line[:50]}...'")
        
        # 空行やページ番号はスキップ
        if not line or 'page=' in line or '©' in line:
            print(f"  → スキップ（空行/ページ番号）")
            i += 1
            continue
        
        # ヘッダーの各カラム名はスキップ
        skip_keywords = ['利率', '（年）', '償還期限', '発行額', '（額面金額）', 
                        '発行の根拠法律及びその条項', '利率（年）', '名称及び記号']
        if line in skip_keywords:
            print(f"  → スキップ（ヘッダーカラム名）")
            i += 1
            continue
        
        # 銘柄行かどうかチェック
        if '利付国庫債券' in line:
            sub_index += 1
            print(f"  ✅ 銘柄行を発見（{sub_index}個目）")
            
            # 次の数行を取得
            rows_needed = format_type
            data_lines = []
            
            print(f"  次の{rows_needed}行を取得:")
            for j in range(rows_needed):
                if i + j < len(lines):
                    data_lines.append(lines[i + j].strip())
                    print(f"    行{i+j}: '{lines[i + j].strip()[:50]}'")
            
            print(f"  → {len(data_lines)}行取得")
            
            # 次の銘柄へ移動
            i += rows_needed
            bonds.append(f"銘柄{sub_index}")  # ダミーデータ
        else:
            print(f"  → スキップ（銘柄行ではない）")
            i += 1
    
    print(f"\n📊 合計: {len(bonds)}銘柄を発見")

if __name__ == "__main__":
    print("\n🐛 TableParser デバッグ v3")
    
    # 5列フォーマット
    test_file = Path("tests/fixtures/利付債券（別表5列）.txt")
    debug_parse_table(test_file)
    
    print("\n" * 3)
    
    # 4列フォーマット
    test_file = Path("tests/fixtures/利付債券（別表4列）.txt")
    debug_parse_table(test_file)