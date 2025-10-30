"""
国庫短期証券の抽出テスト
"""

import logging
import sys
from pathlib import Path

# ログ設定
logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)s:%(name)s:%(message)s'
)

# パーサーをインポート
from parsers.table_parser import TableParser

def test_tanki_bond():
    """国庫短期証券のテスト"""
    
    # テストファイルのパス
    test_file = Path("G:/マイドライブ/JGBデータ/2023/20230420_令和5年5月10日付（財務省第百三十九号）.txt")
    
    print("="*70)
    print("🧪 国庫短期証券抽出テスト")
    print("="*70)
    print(f"📄 ファイル: {test_file.name}")
    print()
    
    # ファイル読み込み
    try:
        with open(test_file, 'r', encoding='utf-8') as f:
            content = f.read()
        print(f"✅ ファイル読み込み成功（{len(content)}文字）")
    except Exception as e:
        print(f"❌ ファイル読み込みエラー: {e}")
        return
    
    print()
    print("-"*70)
    print("🔍 名称部分の確認")
    print("-"*70)
    
    # 名称部分を抽出して表示
    import re
    name_match = re.search(
        r'[１1]\s*名称及び記号\s*(.+?)(?:\n\s*[２2]|\n\n|発行の根拠)', 
        content, 
        re.DOTALL
    )
    if name_match:
        name_text = name_match.group(1).strip()
        print(f"抽出された名称（元のテキスト）:")
        print(f"  {repr(name_text[:200])}")
        print()
        
        # 整形後
        name_clean = re.sub(r'\s+', '', name_text)
        print(f"整形後:")
        print(f"  {name_clean[:200]}")
        print()
        
        # キーワードチェック
        print("キーワードチェック:")
        print(f"  '国庫短期証券' in name_clean: {'国庫短期証券' in name_clean}")
        print(f"  '割引短期国債' in name_clean: {'割引短期国債' in name_clean}")
        print()
    else:
        print("❌ 名称及び記号が見つかりませんでした")
        return
    
    print("-"*70)
    print("🚀 TableParserで抽出開始")
    print("-"*70)
    print()
    
    # パーサー実行
    parser = TableParser()
    result = parser.extract_bond_info_from_single(content)
    
    print()
    print("-"*70)
    print("📊 抽出結果")
    print("-"*70)
    
    if result:
        print(f"✅ 抽出成功")
        print(f"  bond_type: {result.bond_type}")
        print(f"  series_number: {result.series_number}")
        print(f"  bond_name: {result.bond_name}")
        print(f"  interest_rate: {result.interest_rate}")
        print(f"  maturity_date: {result.maturity_date}")
        print()
        
        # 期待値チェック
        if result.bond_type == "短期証券":
            print("🎉 期待通り「短期証券」として抽出されました！")
        elif result.bond_type == "短期":
            print("⚠️ 「短期」として抽出されました（改善前の動作）")
        elif result.bond_type == "短期年":
            print("❌ 「短期年」として抽出されました（問題のパターン）")
        else:
            print(f"⚠️ 予想外の値: {result.bond_type}")
    else:
        print("❌ 抽出失敗")
    
    print("="*70)

if __name__ == '__main__':
    test_tanki_bond()