"""
Phase 5 全件処理 - ステージ4（最終）: 残りのファイル（151～179）

このスクリプトは、全件処理の最終ステージです。
残り29ファイルを処理し、179ファイルすべての処理を完了させます。

実行前の確認事項:
1. Stage 3が正常に完了していること
2. 累計150ファイルのデータがBigQueryで確認できること
3. Stage 3でエラーがなかったこと

実行後の確認事項:
1. 179ファイルすべてが処理されたことを確認
2. 財務省統計との照合準備
3. 最終データ品質チェック
"""

from pathlib import Path
from datetime import datetime
import json
import sys
sys.path.append(str(Path(__file__).parent))

from universal_announcement_parser_v5 import batch_process

# 設定
INPUT_DIR = Path(r"G:\マイドライブ\JGBデータ\2023")
DATASET_ID = "20251028"
STAGE_NAME = "Stage 4 (Final)"
MAX_FILES = 179  # 全件

# ログファイルの設定
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOG_DIR / f"stage4_final_processing_{TIMESTAMP}.json"

print("=" * 70)
print(f"Phase 5 全件処理 - {STAGE_NAME}")
print("=" * 70)
print(f"📁 入力ディレクトリ: {INPUT_DIR}")
print(f"🗄️  データセットID: {DATASET_ID}")
print(f"📊 処理ファイル数: 151～179（29ファイル）")
print(f"📝 ログファイル: {LOG_FILE}")
print("=" * 70)
print()

# Stage 3の確認
print("⚠️  Stage 3の確認:")
print("  Stage 3は正常に完了しましたか？")
print("  累計150ファイルのデータはBigQueryで確認できましたか？")
print("  Stage 3でエラーはありませんでしたか？")
print()
print("🎯 これは最終ステージです。")
print("   完了後、179ファイルすべての処理が終了します。")
print()
response = input("準備ができたらEnterキーを押してください（キャンセルする場合はCtrl+C）...")
print()

# 処理開始
print("🚀 Stage 4（最終ステージ）を開始します...")
print("   ファイル151～179を処理します")
print()

start_time = datetime.now()

# バッチ処理を実行（test_mode=Falseで全件処理モード）
stats = batch_process(
    input_dir=INPUT_DIR,
    dataset_id=DATASET_ID,
    test_mode=False,  # 全件処理モード
    max_files=MAX_FILES,
    insert_to_bq=True
)

end_time = datetime.now()
duration = end_time - start_time

# 結果のサマリー
print()
print("=" * 70)
print("🎉 Phase 5 全件処理完了！")
print("=" * 70)
print(f"処理時間: {duration}")
print(f"最終処理: 29ファイル")
print(f"総処理数: {stats['total']}ファイル")
print(f"成功: {stats['success']} ({stats['success']/stats['total']*100:.1f}%)")
print(f"失敗: {stats['failed']} ({stats['failed']/stats['total']*100:.1f}%)")
print(f"BigQuery投入: {stats['inserted']} ({stats['inserted']/stats['total']*100:.1f}%)")
print()

print("パターン別の最終分布:")
for pattern, count in sorted(stats['by_pattern'].items(), key=lambda x: x[1], reverse=True):
    print(f"  {pattern}: {count} ({count/stats['total']*100:.1f}%)")
print()

if stats['errors']:
    print(f"⚠️  エラーが発生したファイル: {len(stats['errors'])}件")
    print("詳細:")
    for error in stats['errors']:
        print(f"  - {error['file']}")
        print(f"    パターン: {error['pattern']}")
        print(f"    エラー: {error['error'][:100]}...")
    print()
else:
    print("✅ すべてのファイルが正常に処理されました！")
    print()

# ログファイルに保存
log_data = {
    'stage': STAGE_NAME,
    'dataset_id': DATASET_ID,
    'start_time': start_time.isoformat(),
    'end_time': end_time.isoformat(),
    'duration_seconds': duration.total_seconds(),
    'stats': stats,
    'completion_status': 'SUCCESS' if stats['failed'] == 0 else 'PARTIAL'
}

with open(LOG_FILE, 'w', encoding='utf-8') as f:
    json.dump(log_data, f, ensure_ascii=False, indent=2)

print(f"📝 最終ログを保存しました: {LOG_FILE}")
print()

# 最終チェックの案内
print("=" * 70)
print("📊 最終データチェック")
print("=" * 70)
print()
print("次のステップ:")
print()
print("1. 全件処理の確認:")
print()
print("   -- 総件数の確認（179件になっているはず）")
print("   SELECT COUNT(*) as total_count")
print("   FROM `jgb2023.20251028.raw_announcements`")
print("   WHERE issue_date >= DATE('2023-01-01');")
print()
print("2. パターン別の最終分布:")
print()
print("   SELECT ")
print("     format_pattern,")
print("     COUNT(*) as count,")
print("     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage")
print("   FROM `jgb2023.20251028.raw_announcements`")
print("   WHERE issue_date >= DATE('2023-01-01')")
print("   GROUP BY format_pattern")
print("   ORDER BY count DESC;")
print()
print("3. 根拠法令別の発行額集計（財務省統計との照合準備）:")
print()
print("   SELECT ")
print("     law_key,")
print("     ROUND(SUM(issue_amount) / 1000000000000, 2) as amount_trillion_yen,")
print("     COUNT(*) as issuance_count")
print("   FROM `jgb2023.20251028.bond_issuances_by_law`")
print("   GROUP BY law_key")
print("   ORDER BY amount_trillion_yen DESC;")
print()
print("4. 総発行額の計算:")
print()
print("   SELECT ")
print("     ROUND(SUM(issue_amount) / 1000000000000, 2) as total_trillion_yen")
print("   FROM `jgb2023.20251028.bond_issuances_by_law`;")
print()
print("   💡 目標: 財務省統計193.46兆円との照合")
print()

if stats['failed'] == 0:
    print("=" * 70)
    print("🎊 おめでとうございます！")
    print("=" * 70)
    print()
    print("Phase 5の全件処理が正常に完了しました。")
    print("次は財務省統計193.46兆円との照合を行います。")
    print()
    print("Phase 6: データ検証と財務省統計との照合")
    print("  - 根拠法令から国債種別へのマッピング")
    print("  - 発行額の集計と検証")
    print("  - 差異の分析と解消")
else:
    print("=" * 70)
    print("⚠️  一部のファイルでエラーが発生しました")
    print("=" * 70)
    print()
    print("エラーファイルを個別に調査し、")
    print("必要に応じて修正または手動処理を行ってください。")

print()
print("=" * 70)