# test_bigquery_insert_5files.py
from pathlib import Path
from universal_announcement_parser_v5 import batch_process

input_dir = Path(r"G:\マイドライブ\JGBデータ\2023")

# 5ファイルでBigQuery投入テスト
stats = batch_process(
    input_dir=input_dir,
    test_mode=True,
    max_files=5,
    insert_to_bq=True  # BigQuery投入を有効化
)

print("\n" + "=" * 70)
print("📊 最終統計")
print("=" * 70)
print(f"成功率: {stats['success']}/{stats['total']} ({stats['success']/stats['total']*100:.1f}%)")
print(f"BigQuery投入: {stats['inserted']}/{stats['total']} ({stats['inserted']/stats['total']*100:.1f}%)")