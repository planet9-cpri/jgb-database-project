# test_batch_10files.py
from pathlib import Path
from universal_announcement_parser_v5 import batch_process

# 入力ディレクトリ
input_dir = Path(r"G:\マイドライブ\JGBデータ\2023")

# 10ファイルでバッチテスト（BigQuery投入は無効）
stats = batch_process(
    input_dir=input_dir,
    test_mode=True,
    max_files=10,
    insert_to_bq=False  # まずはパースのみテスト
)

print("\n" + "=" * 70)
print("📊 最終統計")
print("=" * 70)
print(f"成功率: {stats['success']}/{stats['total']} ({stats['success']/stats['total']*100:.1f}%)")
print(f"失敗率: {stats['failed']}/{stats['total']} ({stats['failed']/stats['total']*100:.1f}%)")