"""
NoticeParser統合テスト
縦並び形式対応の確認
"""

import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from jgb_parser.notice_parser import NoticeParser


def test_notice_parser():
    """NoticeParserの統合テスト"""
    
    print("=" * 70)
    print("🚀 NoticeParser 統合テスト（縦並び形式対応）")
    print("=" * 70)
    
    # テストファイル
    test_cases = [
        {
            'file': r"G:\マイドライブ\JGBデータ\2023\20230414_令和5年5月9日付（財務省第百二十七号）.txt",
            'expected': 29,
            'format': '縦並び5列形式'
        },
        {
            'file': r"G:\マイドライブ\JGBデータ\2023\20230419_令和5年5月9日付（財務省第百二十六号）.txt",
            'expected': 30,
            'format': '縦並び4列形式'
        }
    ]
    
    results = []
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{'='*70}")
        print(f"📄 テストケース {i}: {test_case['format']}")
        print(f"{'='*70}")
        print(f"ファイル: {Path(test_case['file']).name}")
        
        # ファイルの存在確認
        if not Path(test_case['file']).exists():
            print(f"❌ ファイルが見つかりません: {test_case['file']}")
            results.append(False)
            continue
        
        # パース実行
        try:
            parser = NoticeParser(test_case['file'])
            result = parser.parse()
            
            # 結果表示
            print(f"\n【告示情報】")
            notice = result['notice_info']
            print(f"  告示番号: {notice['notice_number']}")
            print(f"  発行日: {notice['issue_date']}")
            print(f"  公布日: {notice['publication_date']}")
            print(f"  大臣名: {notice['minister_name']}")
            if notice['total_amount']:
                print(f"  総発行額: {notice['total_amount']:,}円")
            
            print(f"\n【銘柄情報】")
            issues = result['issues']
            print(f"  抽出された銘柄数: {len(issues)}")
            print(f"  期待値: {test_case['expected']}")
            
            # 成功判定
            success = len(issues) == test_case['expected']
            
            if success:
                print(f"  ✅ テスト成功！")
            else:
                print(f"  ❌ テスト失敗：期待値と一致しません")
            
            # 最初の3銘柄を表示
            if issues:
                print(f"\n  最初の3銘柄:")
                for j, issue in enumerate(issues[:3], 1):
                    print(f"    {j}. {issue['name']}")
                    print(f"       種類: {issue['bond_type']}, 回号: 第{issue['series_number']}回")
                    print(f"       利率: {issue['rate']}%, 発行額: {issue['amount']:,}円")
                
                if len(issues) > 3:
                    print(f"    ... 他{len(issues) - 3}銘柄")
            
            results.append(success)
            
        except Exception as e:
            print(f"\n❌ エラーが発生しました: {e}")
            import traceback
            traceback.print_exc()
            results.append(False)
    
    # 総合結果
    print(f"\n{'='*70}")
    print("📊 テスト結果サマリー")
    print(f"{'='*70}")
    print(f"総テスト数: {len(results)}")
    print(f"成功: {sum(results)}")
    print(f"失敗: {len(results) - sum(results)}")
    
    if all(results):
        print("\n🎉 全テスト成功！NoticeParserの統合完了！")
        return 0
    else:
        print("\n⚠️ 一部テストが失敗しました")
        return 1


if __name__ == '__main__':
    exit_code = test_notice_parser()
    sys.exit(exit_code)