"""
1つのファイルで詳細デバッグ
"""
import os
import sys

# parsersフォルダをパスに追加
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
parsers_dir = os.path.join(project_root, 'parsers')
sys.path.insert(0, parsers_dir)

from legal_basis_extractor_v2_debug import extract_legal_bases_structured

# テストファイル
file_path = os.path.join(current_dir, "fixtures", "20240220_令和6年3月8日付（財務省第六十三号）.txt")

print("=" * 80)
print("1ファイル詳細デバッグ")
print("=" * 80)
print(f"ファイル: {file_path}")
print(f"存在: {os.path.exists(file_path)}")
print("=" * 80)

# ファイル読み込み
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

print("\n【ファイル内容の一部】")
print("-" * 80)
# 「発行の根拠法律及びその条項」の周辺を表示
import re
section_search = re.search(r'.{0,100}発行の根拠法律及びその条項.{0,300}', content, re.DOTALL)
if section_search:
    print(section_search.group(0))
    print("\n（repr形式）")
    print(repr(section_search.group(0)))
else:
    print("「発行の根拠法律及びその条項」が見つかりませんでした")
print("-" * 80)

# 抽出実行
print("\n【抽出処理開始】")
results = extract_legal_bases_structured(content)

print("\n【最終結果】")
print("=" * 80)
if results:
    print(f"✅ 抽出成功: {len(results)}件")
    for r in results:
        print(f"  - {r['basis']}")
else:
    print("❌ 抽出結果なし")
print("=" * 80)