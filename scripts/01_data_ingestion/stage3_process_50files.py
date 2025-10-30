"""
Phase 5 全件処理 - ステージ3: 次の50ファイル（101～150）

このスクリプトは、Stage 2の成功を確認した後に実行します。
ファイル101～150を処理し、データセット20251028に追加投入します。

実行前の確認事項:
1. Stage 2が正常に完了していること
2. 累計100ファイルのデータがBigQueryで確認できること
3. Stage 2でエラーがなかったこと

実行後の確認事項:
1. 合計150ファイルが処理されたことを確認
2. データの整合性を確認
3. 根拠法令別の発行額が合理的な範囲であることを確認
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
STAGE_NAME = "Stage 3"
MAX_FILES = 150  # 累計150ファイル

# ログファイルの設定
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOG_DIR / f"stage3_processing_{TIMESTAMP}.json"

print("=" * 70)
print(f"Phase 5 全件処理 - {STAGE_NAME}")
print("=" * 70)
print(f"📁 入力ディレクトリ: {INPUT_DIR}")
print(f"🗄️  データセットID: {DATASET_ID}")
print(f"📊 処理ファイル数: 101～150（50ファイル）")
print(f"📝 ログファイル: {LOG_FILE}")
print("=" * 70)
print()

# Stage 2の確認
print("⚠️  Stage 2の確認:")
print("  Stage 2は正常に完了しましたか？")
print("  累計100ファイルのデータはBigQueryで確認できましたか？")
print("  Stage 2でエラーはありませんでしたか？")
print()
response = input("すべて確認できたらEnterキーを押してください（キャンセルする場合はCtrl+C）...")
print()

# 処理開始
print("🚀 Stage 3 を開始します...")
print("   ファイル101～150を処理します")
print()

start_time = datetime.now()

# バッチ処理を実行（累計150ファイル）
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
print("📊 Stage 3 処理結果サマリー")
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
print("✅ Stage 3 完了")
print("=" * 70)
print()
print("進捗状況: 150/179ファイル (83.8%)")
print()
print("次のステップ:")
print("1. BigQueryでデータ品質を確認:")
print()
print("   -- 根拠法令別の発行額を集計（サンプル）")
print("   -- この段階で財務省統計との大まかな比較が可能")
print()
print("2. 使用するビュー: bond_issuances_by_law")
print()
print("   SELECT ")
print("     law_key,")
print("     SUM(issue_amount) / 1000000000000 as amount_trillion")
print("   FROM `jgb2023.20251028.bond_issuances_by_law`")
print("   GROUP BY law_key")
print("   ORDER BY amount_trillion DESC;")
print()

if stats['failed'] == 0:
    print("3. エラーがなかったので、最終ステージ（Stage 4）に進むことができます")
    print("   stage4_process_remaining.py を実行してください")
    print("   残り29ファイルを処理して、全件処理を完了させます")
else:
    print("3. エラーがあったファイルを調査してください")
    print("   問題を解決してから最終ステージに進んでください")

print()
print("=" * 70)