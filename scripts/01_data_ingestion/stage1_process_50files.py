"""
Phase 5 全件処理 - ステージ1: 最初の50ファイル

このスクリプトは、179ファイルの全件処理を段階的に行う第一段階です。
最初の50ファイルを処理し、新しいデータセット20251028に投入します。

実行前の確認事項:
1. 新しいデータセット20251028が作成されていること
2. raw_announcements と announcement_items テーブルが存在すること
3. 統合パーサーv5が正しく配置されていること

実行後の確認事項:
1. BigQueryコンソールで投入されたデータを確認
2. パターン別の分布を確認
3. エラーがあれば詳細を調査
"""

from pathlib import Path
from datetime import datetime
import json

# 統合パーサーをインポート
# 注: universal_announcement_parser_v5.pyと同じディレクトリに配置すること
import sys
sys.path.append(str(Path(__file__).parent))

from universal_announcement_parser_v5 import batch_process

# 設定
INPUT_DIR = Path(r"G:\マイドライブ\JGBデータ\2023")
DATASET_ID = "20251028"  # 新しいデータセット
STAGE_NAME = "Stage 1"
MAX_FILES = 50

# ログファイルの設定
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOG_DIR / f"stage1_processing_{TIMESTAMP}.json"

print("=" * 70)
print(f"Phase 5 全件処理 - {STAGE_NAME}")
print("=" * 70)
print(f"📁 入力ディレクトリ: {INPUT_DIR}")
print(f"🗄️  データセットID: {DATASET_ID}")
print(f"📊 処理ファイル数: {MAX_FILES}ファイル")
print(f"📝 ログファイル: {LOG_FILE}")
print("=" * 70)
print()

# 確認メッセージ
print("⚠️  重要な確認事項:")
print("  1. データセット20251028が作成されていますか？")
print("  2. raw_announcements テーブルは存在しますか？")
print("  3. announcement_items テーブルは存在しますか？")
print()
input("準備ができたらEnterキーを押してください...")
print()

# 処理開始
print("🚀 処理を開始します...")
print()

start_time = datetime.now()

# バッチ処理を実行
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
print("📊 Stage 1 処理結果サマリー")
print("=" * 70)
print(f"処理時間: {duration}")
print(f"総ファイル数: {stats['total']}")
print(f"成功: {stats['success']} ({stats['success']/stats['total']*100:.1f}%)")
print(f"失敗: {stats['failed']} ({stats['failed']/stats['total']*100:.1f}%)")
print(f"BigQuery投入: {stats['inserted']} ({stats['inserted']/stats['total']*100:.1f}%)")
print()

print("パターン別の分布:")
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
print("✅ Stage 1 完了")
print("=" * 70)
print()
print("次のステップ:")
print("1. BigQueryコンソールで以下のクエリを実行してデータを確認してください:")
print()
print("   -- 投入されたデータの確認")
print("   SELECT ")
print("     announcement_id,")
print("     announcement_date,")
print("     issue_date,")
print("     format_pattern,")
print("     format_pattern_confidence")
print("   FROM `jgb2023.20251028.raw_announcements`")
print("   WHERE issue_date >= DATE('2023-01-01')")
print("   ORDER BY announcement_date DESC")
print("   LIMIT 20;")
print()
print("2. パターン別の件数を確認:")
print()
print("   SELECT ")
print("     format_pattern,")
print("     COUNT(*) as count")
print("   FROM `jgb2023.20251028.raw_announcements`")
print("   WHERE issue_date >= DATE('2023-01-01')")
print("   GROUP BY format_pattern")
print("   ORDER BY count DESC;")
print()

if stats['failed'] == 0:
    print("3. エラーがなかったので、Stage 2に進むことができます")
    print("   stage2_process_50files.py を実行してください")
else:
    print("3. エラーがあったファイルを調査してください")
    print("   問題を解決してから次のステージに進んでください")

print()
print("=" * 70)