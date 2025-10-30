"""
VerticalTableParserの実ファイルテスト
Day 4で追加
"""

import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from jgb_parser.vertical_table_parser import VerticalTableParser


def test_file(file_path: str, expected_count: int, description: str):
    """ファイルをテスト"""
    print("\n" + "=" * 70)
    print(f"📄 {description}")
    print("=" * 70)
    print(f"ファイル: {file_path}")
    
    # ファイルを読み込み
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            notice_text = f.read()
    except FileNotFoundError:
        print(f"❌ ファイルが見つかりません: {file_path}")
        return False
    
    # パース実行
    parser = VerticalTableParser(notice_text)
    issues = parser.parse()
    
    # 結果表示
    print(f"\n✅ 抽出された銘柄数: {len(issues)}")
    print(f"📊 期待値: {expected_count}")
    print(f"列形式: {parser.column_count}列")
    
    if parser.common_legal_basis:
        print(f"共通法令根拠: {parser.common_legal_basis}")
    
    # 成功/失敗判定
    success = len(issues) == expected_count
    
    if success:
        print(f"✅ テスト成功！")
    else:
        print(f"❌ テスト失敗：期待値と一致しません")
    
    # 最初の3銘柄を詳細表示
    print(f"\n📋 最初の3銘柄（詳細）:")
    for i, issue in enumerate(issues[:3], 1):
        print(f"\n  {i}. {issue['name']}")
        print(f"     種類: {issue['bond_type']}")
        print(f"     回号: 第{issue['series_number']}回")
        print(f"     利率: {issue['rate']}%")
        print(f"     償還期限: {issue['maturity_date'].strftime('%Y年%m月%d日') if issue['maturity_date'] else 'None'}")
        print(f"     発行額: {issue['amount']:,}円" if issue['amount'] else "     発行額: None")
        print(f"     法令根拠: {issue['legal_basis']}")
    
    # 全銘柄のサマリー
    if len(issues) > 3:
        print(f"\n  ... 他{len(issues) - 3}銘柄")
    
    return success


def main():
    """メイン実行"""
    print("🚀 VerticalTableParser 実ファイルテスト")
    print("=" * 70)
    
    # Googleドライブのパスを設定（環境に応じて調整してください）
    data_dir = Path("G:/マイドライブ/JGBデータ/2023")
    
    # テストケース
    test_cases = [
        {
            'file': data_dir / "20230414_令和5年5月9日付（財務省第百二十七号）.txt",
            'expected_count': 29,  # 20年×4 + 30年×17 + 40年×8
            'description': "令和5年4月14日発行（5列形式）"
        },
        {
            'file': data_dir / "20230419_令和5年5月9日付（財務省第百二十六号）.txt",
            'expected_count': 30,  # 10年×6 + 20年×14 + 30年×10
            'description': "令和5年4月19日発行（4列形式）"
        }
    ]
    
    # テスト実行
    results = []
    for test_case in test_cases:
        success = test_file(
            str(test_case['file']),
            test_case['expected_count'],
            test_case['description']
        )
        results.append(success)
    
    # 総合結果
    print("\n" + "=" * 70)
    print("📊 テスト結果サマリー")
    print("=" * 70)
    
    total = len(results)
    passed = sum(results)
    
    print(f"総テスト数: {total}")
    print(f"成功: {passed}")
    print(f"失敗: {total - passed}")
    
    if all(results):
        print("\n🎉 全テスト成功！")
        return 0
    else:
        print("\n⚠️ 一部テストが失敗しました")
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)