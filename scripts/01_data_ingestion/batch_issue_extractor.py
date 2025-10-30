"""
一括銘柄抽出スクリプト
Day 4：残り24件の告示を処理
"""

import sys
import logging
from pathlib import Path
from typing import List, Dict
import json

# パスの設定
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from parsers.issue_extractor import IssueExtractor

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# 銘柄なし告示リスト（BigQueryから取得）
UNPROCESSED_NOTICES = [
    "20230414_令和5年5月9日付（財務省第百二十七号）.txt",
    "20230419_令和5年5月9日付（財務省第百二十六号）.txt",
    "20230508_令和5年6月8日付（財務省第百五十八号）.txt",
    "20230522_令和5年6月8日付（財務省第百五十九号）.txt",
    "20230609_令和5年7月11日付（財務省第百八十六号）.txt",
    "20230614_令和5年7月11日付（財務省第百八十七号）.txt",
    "20230720_令和5年8月8日付（財務省第二百一号）.txt",
    "20230724_令和5年8月8日付（財務省第二百二号）.txt",
    "20230823_令和5年9月7日付（財務省第二百二十二号）.txt",
    "20230825_令和5年9月7日付（財務省第二百二十三号）.txt",
    "20230920_令和5年10月11日付（財務省第二百五十四号）.txt",
    "20230922_令和5年10月11日付（財務省第二百五十五号）.txt",
    "20231020_令和5年11月9日付（財務省第二百七十四号）.txt",
    "20231026_令和5年11月9日付（財務省第二百七十五号）.txt",
    "20231117_令和5年12月12日付（財務省第三百六号）.txt",
    "20231127_令和5年12月12日付（財務省第三百七号）.txt",
    "20231221_令和6年1月12日付（財務省第七号）.txt",
    "20231225_令和6年1月12日付（財務省第八号）.txt",
    "20240109_令和6年2月8日付（財務省第三十八号）.txt",
    "20240123_令和6年2月8日付（財務省第三十九号）.txt",
    "20240219_令和6年3月12日付（財務省第七十四号）.txt",
    "20240226_令和6年3月12日付（財務省第七十三号）.txt",
    "20240322_令和6年4月10日付（財務省第百十二号）.txt",
    "20240326_令和6年4月10日付（財務省第百十一号）.txt",
]


def process_notices(data_dir: str) -> Dict:
    """
    銘柄なし告示を一括処理
    
    Args:
        data_dir: JGBデータディレクトリのパス
        
    Returns:
        処理結果のサマリー
    """
    data_dir = Path(data_dir)
    
    results = {
        'success': [],  # 抽出成功
        'failed': [],   # 抽出失敗
        'not_found': []  # ファイル未発見
    }
    
    print("=" * 70)
    print("🚀 一括銘柄抽出処理")
    print("=" * 70)
    print(f"対象ファイル数: {len(UNPROCESSED_NOTICES)}")
    print(f"データディレクトリ: {data_dir}")
    print()
    
    for i, filename in enumerate(UNPROCESSED_NOTICES, 1):
        print(f"\n[{i}/{len(UNPROCESSED_NOTICES)}] {filename}")
        print("-" * 70)
        
        # ファイルパスを構築
        # 発行日ベースで判定（2023年度 = 2023/4〜2024/3）
        issue_date = filename[:8]  # YYYYMMDD
        year = int(issue_date[:4])
        month = int(issue_date[4:6])
        
        # 年度判定：4月〜翌年3月を同じ年度とする
        if month >= 4:
            fiscal_year = year
        else:
            fiscal_year = year - 1
        
        filepath = data_dir / str(fiscal_year) / filename
        
        # ファイル存在確認
        if not filepath.exists():
            print(f"❌ ファイルが見つかりません: {filepath}")
            results['not_found'].append({
                'filename': filename,
                'filepath': str(filepath)
            })
            continue
        
        # 銘柄抽出
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                notice_text = f.read()
            
            extractor = IssueExtractor(notice_text)
            issues = extractor.extract_issues()
            
            if issues:
                print(f"✅ 抽出成功：{len(issues)}銘柄")
                
                # 最初の3銘柄を表示
                for j, issue in enumerate(issues[:3], 1):
                    print(f"  {j}. {issue['name']}")
                    print(f"     種類: {issue['bond_type']}, 回号: 第{issue['series_number']}回")
                    print(f"     利率: {issue['rate']}%, 発行額: {issue['amount']:,}円")
                
                if len(issues) > 3:
                    print(f"  ... 他{len(issues) - 3}銘柄")
                
                results['success'].append({
                    'filename': filename,
                    'filepath': str(filepath),
                    'issue_count': len(issues),
                    'issues': issues
                })
            else:
                print(f"⚠️ 銘柄を抽出できませんでした")
                results['failed'].append({
                    'filename': filename,
                    'filepath': str(filepath)
                })
        
        except Exception as e:
            print(f"❌ エラーが発生しました: {e}")
            logger.exception(f"処理エラー: {filename}")
            results['failed'].append({
                'filename': filename,
                'filepath': str(filepath),
                'error': str(e)
            })
    
    return results


def print_summary(results: Dict):
    """処理結果のサマリーを表示"""
    print("\n" + "=" * 70)
    print("📊 処理結果サマリー")
    print("=" * 70)
    
    success_count = len(results['success'])
    failed_count = len(results['failed'])
    not_found_count = len(results['not_found'])
    total = success_count + failed_count + not_found_count
    
    print(f"\n【総合結果】")
    print(f"  総ファイル数: {total}")
    print(f"  ✅ 抽出成功: {success_count} ({success_count/total*100:.1f}%)")
    print(f"  ⚠️ 抽出失敗: {failed_count} ({failed_count/total*100:.1f}%)")
    print(f"  ❌ 未発見: {not_found_count} ({not_found_count/total*100:.1f}%)")
    
    # 抽出成功ファイルの詳細
    if results['success']:
        print(f"\n【抽出成功ファイル】({success_count}件)")
        total_issues = 0
        for item in results['success']:
            print(f"  ✅ {item['filename']}: {item['issue_count']}銘柄")
            total_issues += item['issue_count']
        print(f"  総銘柄数: {total_issues}銘柄")
    
    # 抽出失敗ファイルの詳細
    if results['failed']:
        print(f"\n【抽出失敗ファイル】({failed_count}件)")
        for item in results['failed']:
            error_msg = f" - {item.get('error', '')}" if 'error' in item else ""
            print(f"  ⚠️ {item['filename']}{error_msg}")
    
    # 未発見ファイルの詳細
    if results['not_found']:
        print(f"\n【未発見ファイル】({not_found_count}件)")
        for item in results['not_found']:
            print(f"  ❌ {item['filename']}")
    
    print("\n" + "=" * 70)


def save_results(results: Dict, output_file: str = "extraction_results.json"):
    """結果をJSONファイルに保存"""
    output_path = Path(output_file)
    
    # datetimeをstrに変換
    def convert_datetime(obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return obj
    
    # 結果を整形
    output_data = {
        'success': results['success'],
        'failed': results['failed'],
        'not_found': results['not_found'],
        'summary': {
            'total': len(results['success']) + len(results['failed']) + len(results['not_found']),
            'success_count': len(results['success']),
            'failed_count': len(results['failed']),
            'not_found_count': len(results['not_found']),
            'total_issues': sum(item['issue_count'] for item in results['success'])
        }
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2, default=convert_datetime)
    
    print(f"\n💾 結果を保存しました: {output_path}")


def main():
    """メイン実行"""
    # データディレクトリ
    data_dir = r"G:\マイドライブ\JGBデータ"
    
    # 一括処理実行
    results = process_notices(data_dir)
    
    # サマリー表示
    print_summary(results)
    
    # 結果をJSON保存
    save_results(results)
    
    # 終了コード
    if results['failed'] or results['not_found']:
        return 1
    return 0


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)