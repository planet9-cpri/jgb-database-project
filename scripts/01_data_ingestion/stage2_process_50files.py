"""
Phase 5 全件処理 - ステージ2: 次の50ファイル（51～100）

このスクリプトは、Stage 1の成功を確認した後に実行します。
ファイル51～100を処理し、データセット20251028に追加投入します。

実行前の確認事項:
1. Stage 1が正常に完了していること
2. Stage 1のデータがBigQueryで確認できること
3. Stage 1でエラーがなかったこと

実行後の確認事項:
1. 合計100ファイルが処理されたことを確認
2. パターン別の分布が妥当であることを確認
3. 重複したannouncement_idがないことを確認
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
STAGE_NAME = "Stage 2"
MAX_FILES = 100  # 累計100ファイル（Stage 1の50 + Stage 2の50）

# ログファイルの設定
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOG_DIR / f"stage2_processing_{TIMESTAMP}.json"

print("=" * 70)
print(f"Phase 5 全件処理 - {STAGE_NAME}")
print("=" * 70)
print(f"📁 入力ディレクトリ: {INPUT_DIR}")
print(f"🗄️  データセットID: {DATASET_ID}")
print(f"📊 処理ファイル数: 51～100（50ファイル）")
print(f"📝 ログファイル: {LOG_FILE}")
print("=" * 70)
print()

# Stage 1の確認
print("⚠️  Stage 1の確認:")
print("  Stage 1は正常に完了しましたか？")
print("  Stage 1のデータはBigQueryで確認できましたか？")
print("  Stage 1でエラーはありませんでしたか？")
print()
response = input("すべて確認できたらEnterキーを押してください（キャンセルする場合はCtrl+C）...")
print()

# 処理開始
print("🚀 Stage 2 を開始します...")
print("   ファイル51～100を処理します")
print()

start_time = datetime.now()

# バッチ処理を実行（Stage 1と合わせて100ファイル）
stats = batch_process(
    input_dir=INPUT_DIR,
    dataset_id=DATASET_ID,
    test_mode=True,
    max_files=MAX_FILES,
    insert_to_bq=True
)

end_time = datetime.now()
duration = end_time - start_time

# 結果のサマリー
print()
print("=" * 70)
print("📊 Stage 2 処理結果サマリー")
print("=" * 70)
print(f"処理時間: {duration}")
print(f"今回処理: 50ファイル")
print(f"累計処理: {stats['total']}ファイル")
print(f"成功: {stats['success']} ({stats['success']/stats['total']*100:.1f}%)")
print(f"失敗: {stats['failed']} ({stats['failed']/stats['total']*100:.1f}%)")
print(f"BigQuery投入: {stats['inserted']} ({stats['inserted']/stats['total']*100:.1f}%)")
print()

print("パターン別の分布（累計）:")
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

# ログファイルに保存
log_data = {
    'stage': STAGE_NAME,
    'dataset_id': DATASET_ID,
    'start_time': start_time.isoformat(),
    'end_time': end_time.isoformat(),
    'duration_seconds': duration.total_seconds(),
    'stats': stats
}

with open(LOG_FILE, 'w', encoding='utf-8') as f:
    json.dump(log_data, f, ensure_ascii=False, indent=2)

print(f"📝 詳細ログを保存しました: {LOG_FILE}")
print()

# 次のステップの案内
print("=" * 70)
print("✅ Stage 2 完了")
print("=" * 70)
print()
print("進捗状況: 100/179ファイル (55.9%)")
print()
print("次のステップ:")
print("1. BigQueryで累計データを確認:")
print()
print("   -- 全体の件数確認")
print("   SELECT COUNT(*) as total_count")
print("   FROM `jgb2023.20251028.raw_announcements`")
print("   WHERE issue_date >= DATE('2023-01-01');")
print()
print("   -- パターン別の分布")
print("   SELECT format_pattern, COUNT(*) as count")
print("   FROM `jgb2023.20251028.raw_announcements`")
print("   WHERE issue_date >= DATE('2023-01-01')")
print("   GROUP BY format_pattern;")
print()

if stats['failed'] == 0:
    print("2. エラーがなかったので、Stage 3に進むことができます")
    print("   stage3_process_50files.py を実行してください")
else:
    print("2. エラーがあったファイルを調査してください")
    print("   問題を解決してから次のステージに進んでください")

print()
print("=" * 70)