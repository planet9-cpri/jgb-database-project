"""
実際の告示ファイルでlegal_basis_extractor_v2をテスト

3つの実ファイルで動作確認:
1. 20240220 - 国庫短期証券（複数法的根拠）
2. 20250724 - 利付国庫債券40年（2つの法的根拠）
3. 20240110 - 国庫短期証券＋政府短期証券（混在・複雑）
"""

import os
import sys

# プロジェクトルートのparsersフォルダをパスに追加
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # tests/ の親ディレクトリ
parsers_dir = os.path.join(project_root, 'parsers')
sys.path.insert(0, parsers_dir)

from legal_basis_extractor_v3 import extract_legal_bases_structured  # v2 → v3 に変更

# テスト対象ファイル（fixtures/ フォルダ内）
TEST_FILES = [
    {
        "name": "20240220_令和6年3月8日付（財務省第六十三号）",
        "path": os.path.join("fixtures", "20240220_令和6年3月8日付（財務省第六十三号）.txt"),
        "description": "国庫短期証券（複数法的根拠）",
        "expected": ["年金特例債", "GX経済移行債", "借換債"]
    },
    {
        "name": "20250724_令和7年8月8日付（財務省第二百十三号）",
        "path": os.path.join("fixtures", "20250724_令和7年8月8日付（財務省第二百十三号）.txt"),
        "description": "利付国庫債券40年（2つの法的根拠）",
        "expected": ["4条債", "借換債"]
    },
    {
        "name": "20240110_令和6年2月9日付（財務省第四十七号）",
        "path": os.path.join("fixtures", "20240110_令和6年2月9日付（財務省第四十七号）.txt"),
        "description": "国庫短期証券＋政府短期証券（混在・複雑）",
        "expected": ["借換債"]  # 政府短期証券は未対応の可能性
    }
]


def test_file(file_info):
    """1つのファイルをテスト"""
    print("\n" + "=" * 80)
    print(f"【テスト】{file_info['description']}")
    print("=" * 80)
    print(f"ファイル: {file_info['name']}")
    print(f"期待値: {', '.join(file_info['expected'])}")
    print("-" * 80)
    
    # デバッグ情報
    print(f"カレントディレクトリ: {os.getcwd()}")
    print(f"スクリプトの場所: {os.path.dirname(os.path.abspath(__file__))}")
    print(f"探しているパス: {file_info['path']}")
    
    # スクリプトの場所を基準にした絶対パスに変換
    script_dir = os.path.dirname(os.path.abspath(__file__))
    absolute_path = os.path.join(script_dir, file_info['path'])
    print(f"絶対パス: {absolute_path}")
    print(f"ファイル存在: {os.path.exists(absolute_path)}")
    print("-" * 80)
    
    # ファイルの存在確認（絶対パスを使用）
    if not os.path.exists(absolute_path):
        print(f"❌ ファイルが見つかりません: {absolute_path}")
        return False
    
    # ファイル読み込み（絶対パスを使用）
    try:
        with open(absolute_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"❌ ファイル読み込みエラー: {e}")
        return False
    
    # 発行根拠法令の抽出
    try:
        results = extract_legal_bases_structured(content)
    except Exception as e:
        print(f"❌ 抽出エラー: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # 結果表示
    if results:
        print(f"\n✅ 抽出成功: {len(results)}件")
        print("-" * 80)
        for i, r in enumerate(results, 1):
            print(f"{i}. 発行根拠: {r['basis']}")
            print(f"   大分類: {r['category']}")
            print(f"   詳細分類: {r['sub_category']}")
            print(f"   条項: {r['full']}")
            print()
        
        # 期待値との比較
        extracted_bases = [r['basis'] for r in results]
        print("-" * 80)
        print("【期待値との比較】")
        for expected in file_info['expected']:
            if expected in extracted_bases:
                print(f"  ✅ {expected}: 抽出成功")
            else:
                print(f"  ⚠️ {expected}: 抽出されませんでした")
        
        # 期待外の抽出があるか
        unexpected = [b for b in extracted_bases if b not in file_info['expected']]
        if unexpected:
            print(f"\n  ℹ️ 期待値以外の抽出: {', '.join(unexpected)}")
        
        return True
    else:
        print(f"\n⚠️ 抽出結果なし")
        print("   原因:")
        print("   - 「発行の根拠法律及びその条項」セクションが見つからない")
        print("   - 法律名がパターンにマッチしない")
        print("   - 条項がマッピングテーブルに存在しない")
        return False


def main():
    """メイン処理"""
    print("=" * 80)
    print("実際の告示ファイルでのテスト")
    print("legal_basis_extractor_v2.py の動作確認")
    print("=" * 80)
    
    success_count = 0
    total_count = len(TEST_FILES)
    
    for file_info in TEST_FILES:
        if test_file(file_info):
            success_count += 1
    
    # 総合結果
    print("\n" + "=" * 80)
    print("【総合結果】")
    print("=" * 80)
    print(f"成功: {success_count}/{total_count}")
    
    if success_count == total_count:
        print("🎉 全テスト成功！")
    else:
        print(f"⚠️ {total_count - success_count}件のテストで問題が発生しました")
    
    print("=" * 80)


if __name__ == "__main__":
    main()