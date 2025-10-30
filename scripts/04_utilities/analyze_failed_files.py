"""
失敗ファイルのパターン分析スクリプト

失敗した155ファイルを分類：
- 別表の有無
- 番号付きリスト形式
- 銘柄数の推定
"""

import os
import re
from pathlib import Path
from collections import defaultdict

# 設定
KANPO_DIR = Path(r"G:\マイドライブ\JGBデータ\2023")
RESULT_FILE = Path(r"C:\Users\sonke\projects\jgb_database_project\output\phase3")

# 失敗ファイルのリスト（前のスクリプトから取得）
failed_files = [
    "20230403_令和5年5月9日付（財務省第百二十一号）.txt",
    "20230405_令和5年5月9日付（財務省第百二十三号）.txt",
    "20230407_令和5年5月9日付（財務省第百二十五号）.txt",
    "20230410_令和5年5月9日付（財務省第百二十九号）.txt",
    "20230412_令和5年5月9日付（財務省第百二十二号）.txt",
    # ... 他のファイルも含める（JSONから読み込むか手動で追加）
]

# または、JSONファイルから自動取得
import json

result_files = list(RESULT_FILE.glob("integrated_result_v2_*.json"))
if result_files:
    latest = max(result_files, key=lambda x: x.stat().st_mtime)
    with open(latest, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    failed_files = [r['file_name'] for r in data['results'] 
                   if r['success'] and len(r['integrated_data']) == 0]

print(f"失敗ファイル数: {len(failed_files)}")
print("=" * 80)

# パターン分類
patterns = defaultdict(list)

for i, file_name in enumerate(failed_files[:30], 1):  # 最初の30件を分析
    file_path = KANPO_DIR / file_name
    
    if not file_path.exists():
        print(f"⚠️ ファイルが見つかりません: {file_name}")
        continue
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # パターン検出
    has_table = '別表' in content or '表　' in content
    has_numbered_list = bool(re.search(r'^１\s+.*名称', content, re.MULTILINE))
    has_multiple_names = content.count('利付国庫債券') > 1 or content.count('国庫短期証券') > 1
    
    # 銘柄数の推定
    bond_count = content.count('利付国庫債券') + content.count('国庫短期証券')
    
    # 分類
    if has_numbered_list and not has_table:
        pattern = "番号付きリスト（別表なし）"
    elif has_table and not has_multiple_names:
        pattern = "別表あり（単一銘柄？）"
    elif has_table and has_multiple_names:
        pattern = "別表あり（複数銘柄・特殊形式）"
    else:
        pattern = "その他"
    
    patterns[pattern].append({
        'file_name': file_name,
        'bond_count': bond_count,
    })
    
    # サンプル表示
    if i <= 5:
        print(f"\n[{i}] {file_name}")
        print(f"    パターン: {pattern}")
        print(f"    別表: {'あり' if has_table else 'なし'}")
        print(f"    番号リスト: {'あり' if has_numbered_list else 'なし'}")
        print(f"    推定銘柄数: {bond_count}")

# 集計結果
print("\n" + "=" * 80)
print("【パターン分類結果】")
print("=" * 80)

for pattern, files in sorted(patterns.items(), key=lambda x: -len(x[1])):
    print(f"\n{pattern}: {len(files)}件")
    print(f"  例: {files[0]['file_name']}")
    if len(files) > 1:
        print(f"      {files[1]['file_name']}")

print("\n" + "=" * 80)
print("【次のステップ】")
print("=" * 80)
print("1. 各パターンの代表ファイルを1つずつ詳しく確認")
print("2. v4の設計方針を決定")
print("3. v4実装開始")