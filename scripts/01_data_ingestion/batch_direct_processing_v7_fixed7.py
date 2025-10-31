r"""
直接パース方式バッチ処理スクリプト v7_fixed7 (真・最終完全版)

修正内容 (v7_fixed6→v7_fixed7):
1. サンプル表示: CASEを削除、bond_categoryをそのまま表示
2. 重複抽出防止: 位置バケット方式で同額・近接位置を同一視
3. 許可単位リスト: ALLOWED_UNITS定数を作成、未知単位をスキップ
4. リトライ処理: 指数バックオフでノード混雑時の分散改善

保存場所: C:/Users/sonke/projects/jgb_database_project/scripts/01_data_ingestion/
実行: python batch_direct_processing_v7_fixed7.py --limit 10  # テスト
     python batch_direct_processing_v7_fixed7.py --reset --limit 10  # リセット
     python batch_direct_processing_v7_fixed7.py --limit 0    # 全件
"""

import os
import sys
from pathlib import Path
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition
from datetime import datetime, timezone, timedelta
from uuid import uuid4
import hashlib
import re
import argparse
import logging
import unicodedata
import time
import random
from typing import List, Dict, Tuple

# ===========================
# CLI引数の設定
# ===========================
parser = argparse.ArgumentParser(description='国債データベース バッチ処理スクリプト v7_fixed7')
parser.add_argument('--project', default=os.getenv('BQ_PROJECT', 'jgb2023'))
parser.add_argument('--dataset', default=os.getenv('BQ_DATASET', '20251031'))
parser.add_argument('--location', default=os.getenv('BQ_LOCATION', 'asia-northeast1'))
parser.add_argument('--data-dir', default=os.getenv('DATA_DIR', r'G:\マイドライブ\JGBデータ\2023'))
# 認証情報: デフォルトは環境変数（ADC）に寄せる
parser.add_argument('--credentials', default=os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
parser.add_argument('--limit', type=int, default=10, help='処理件数 (0=全件)')
parser.add_argument('--log-file', default='batch_processing_v7_fixed7.log')
parser.add_argument('--verbose', action='store_true', help='詳細ログ出力')
parser.add_argument('--min-amount', type=int, default=100000000, help='最小金額（円）デフォルト=1億円')
parser.add_argument('--reset', action='store_true', help='既存データを削除して再投入')

args = parser.parse_args()

# 明示指定があるときだけ環境変数を上書き
if args.credentials:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.credentials

# ===========================
# ロギングの設定
# ===========================
log_level = logging.DEBUG if args.verbose else logging.INFO
logging.basicConfig(
    level=log_level,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(args.log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ===========================
# 設定
# ===========================
PROJECT_ID = args.project
DATASET_ID = args.dataset
LOCATION = args.location
DATA_DIR = args.data_dir
MIN_AMOUNT = args.min_amount

# 許可単位リスト（v7_fixed7: 新規追加）
ALLOWED_UNITS = {'兆', '億', '円'}

# 位置バケットサイズ（v7_fixed7: 新規追加）
POSITION_BUCKET_SIZE = 20  # 20文字以内の近接位置は同一視

logger.info("=" * 80)
logger.info("直接パース方式バッチ処理 v7_fixed7 (真・最終完全版)")
logger.info("=" * 80)
logger.info(f"プロジェクトID: {PROJECT_ID}")
logger.info(f"データセットID: {DATASET_ID}")
logger.info(f"データディレクトリ: {DATA_DIR}")
logger.info(f"処理制限: {args.limit}件 (0=全件)")
logger.info(f"最小金額: {MIN_AMOUNT:,}円 ({MIN_AMOUNT/100000000:.0f}億円)")
logger.info(f"許可単位: {', '.join(ALLOWED_UNITS)}")
logger.info(f"位置バケットサイズ: {POSITION_BUCKET_SIZE}文字")
if args.reset:
    logger.warning(f"⚠ リセットモード: 既存データを削除して再投入します")
logger.info("=" * 80)

# ===========================
# ユーティリティ関数
# ===========================
def now_rfc3339() -> str:
    """RFC3339形式のタイムスタンプを生成"""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def snippet_fingerprint(text: str, match, context: int = 24) -> str:
    """
    マッチ周辺の生文脈から指紋を生成
    
    バージョンやパターン名が変わっても、同じ箇所を抽出していれば
    同じ指紋が生成される
    """
    s = max(0, match.start() - context)
    e = min(len(text), match.end() + context)
    frag = re.sub(r'\s+', '', text[s:e])  # 空白除去で安定化
    return hashlib.sha1(frag.encode('utf-8')).hexdigest()

def generate_dedupe_key(announcement_id: str, issue_amount: int, 
                        legal_basis_normalized: str, fingerprint: str) -> str:
    """
    重複排除キーを生成（SHA1ハッシュ）
    
    改善点:
    - sourceを除外（バージョン違いでの重複防止）
    - fingerprintを追加（抽出箇所の指紋）
    """
    normalized = legal_basis_normalized or ""
    basis = f"{announcement_id}|{issue_amount}|{normalized}|{fingerprint}"
    return hashlib.sha1(basis.encode('utf-8')).hexdigest()

def exponential_backoff_sleep(attempt: int, base_delay: float = 1.0, max_delay: float = 60.0):
    """
    指数バックオフでスリープ（v7_fixed7: 新規追加）
    
    ノード混雑時にリトライが集中しないよう、ランダム性を持たせたスリープ
    """
    delay = min(base_delay * (2 ** attempt), max_delay)
    jitter = random.uniform(0, delay * 0.1)  # 最大10%のジッター
    sleep_time = delay + jitter
    logger.debug(f"  指数バックオフ: {sleep_time:.2f}秒スリープ（試行{attempt+1}回目）")
    time.sleep(sleep_time)

# ===========================
# BigQueryクライアント
# ===========================
try:
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    logger.info("✓ BigQueryクライアント初期化完了")
except Exception as e:
    logger.exception(f"✗ BigQueryクライアント初期化エラー")
    sys.exit(1)

# ===========================
# データセットの作成
# ===========================
def ensure_dataset():
    """データセットが存在しない場合は作成"""
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = LOCATION
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"✓ データセット準備完了: {dataset_ref}")
    except Exception as e:
        logger.exception(f"✗ データセット作成エラー")
        sys.exit(1)

ensure_dataset()

# ===========================
# データディレクトリの確認
# ===========================
data_path = Path(DATA_DIR)
if not data_path.exists():
    logger.error(f"✗ データディレクトリが見つかりません: {DATA_DIR}")
    sys.exit(1)

txt_files = sorted(list(data_path.glob("*.txt")))
logger.info(f"✓ .txtファイル: {len(txt_files)}件")

# 処理対象の選択
if args.limit == 0:
    test_files = txt_files
    logger.info(f"全件モード: {len(test_files)}件を処理します")
else:
    test_files = txt_files[:args.limit]
    logger.info(f"制限モード: {len(test_files)}件を処理します")

# ===========================
# テーブルID
# ===========================
table_id_layer2 = f"{PROJECT_ID}.{DATASET_ID}.bond_issuances"
table_id_parse_log = f"{PROJECT_ID}.{DATASET_ID}.parse_log"

# ===========================
# bond_issuancesテーブルの作成（v7_fixed7版）
# ===========================
def ensure_bond_issuances_table():
    """
    bond_issuancesテーブルが存在しない場合は作成（v7_fixed7版）
    
    改善点:
    - --reset時はテーブルを削除→再作成でREQUIREDを保証
    """
    schema = [
        bigquery.SchemaField("announcement_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("bond_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("issue_amount", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("legal_basis", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("legal_basis_normalized", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("legal_basis_source", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("bond_category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("mof_category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("data_quality_score", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("is_summary_record", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("is_detail_record", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("dedupe_key", "STRING", mode="REQUIRED"),  # REQUIREDで定義
    ]
    
    try:
        table = bigquery.Table(table_id_layer2, schema=schema)
        # クラスタリングフィールドの最適化: dedupe_keyを先頭に
        table.clustering_fields = ['dedupe_key', 'announcement_id', 'issue_amount']
        
        # --reset時はテーブルを削除→再作成
        if args.reset:
            logger.info("⚠ リセットモード: テーブルを削除して再作成します...")
            client.delete_table(table_id_layer2, not_found_ok=True)
            logger.info("✓ 既存テーブルを削除しました")
        
        client.create_table(table, exists_ok=True)
        logger.info(f"✓ bond_issuancesテーブル準備完了: {table_id_layer2}")
        
        # PRIMARY KEY制約の追加（--reset時は新規テーブルなので追加）
        if args.reset:
            try:
                pk_sql = f"""
                ALTER TABLE `{table_id_layer2}`
                ADD PRIMARY KEY (dedupe_key) NOT ENFORCED
                """
                client.query(pk_sql, location=LOCATION).result()
                logger.info("✓ PRIMARY KEY制約の追加完了")
            except Exception as e:
                logger.debug(f"✓ PRIMARY KEY制約の追加スキップ: {e}")
        
    except Exception as e:
        logger.exception(f"✗ bond_issuancesテーブル作成エラー")
        sys.exit(1)

ensure_bond_issuances_table()

# ===========================
# dedupe_key列の処理（v7_fixed7版）
# ===========================
def ensure_dedupe_key_column():
    """
    dedupe_key列の処理（v7_fixed7版）
    
    改善点:
    - --reset時はテーブル再作成済みなのでスキップ
    - 非reset時のみバックフィルとPRIMARY KEY追加
    """
    if args.reset:
        # --reset時はテーブル再作成済みなので何もしない
        logger.info("✓ dedupe_key列の準備完了（テーブル再作成済み）")
        return
    
    try:
        logger.info("dedupe_key列の確認中（非リセットモード）...")
        
        # ステップ1: 列が無ければ追加（NULLABLE）
        alter_sql = f"""
        ALTER TABLE `{table_id_layer2}`
        ADD COLUMN IF NOT EXISTS dedupe_key STRING
        """
        client.query(alter_sql, location=LOCATION).result()
        logger.info("✓ dedupe_key列の追加完了（既存の場合はスキップ）")
        
        # ステップ2: 既存行のバックフィル（指紋なしの安定キー）
        backfill_sql = f"""
        UPDATE `{table_id_layer2}`
        SET dedupe_key = TO_HEX(SHA1(CONCAT(
          announcement_id, '|', 
          CAST(issue_amount AS STRING), '|', 
          IFNULL(legal_basis_normalized, ''), '|',
          ''  -- 既存は指紋が無い前提
        )))
        WHERE dedupe_key IS NULL
        """
        result = client.query(backfill_sql, location=LOCATION).result()
        
        # 更新件数を取得
        if hasattr(result, 'num_dml_affected_rows'):
            updated = result.num_dml_affected_rows
            if updated > 0:
                logger.info(f"✓ dedupe_keyバックフィル完了: {updated}件")
            else:
                logger.debug(f"✓ dedupe_keyバックフィル: 更新不要（0件）")
        else:
            logger.debug(f"✓ dedupe_keyバックフィル: 完了")
        
        # ステップ3: PRIMARY KEY制約の追加
        try:
            pk_sql = f"""
            ALTER TABLE `{table_id_layer2}`
            ADD PRIMARY KEY (dedupe_key) NOT ENFORCED
            """
            client.query(pk_sql, location=LOCATION).result()
            logger.info("✓ PRIMARY KEY制約の追加完了")
        except Exception as e:
            if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                logger.debug("✓ PRIMARY KEY制約は既に存在")
            else:
                logger.warning(f"⚠ PRIMARY KEY制約の追加失敗: {e}")
        
    except Exception as e:
        logger.exception(f"✗ dedupe_key列の処理エラー")
        sys.exit(1)

ensure_dedupe_key_column()

# ===========================
# parse_logテーブルの作成
# ===========================
def ensure_parse_log_table():
    """parse_logテーブルが存在しない場合は作成"""
    schema = [
        bigquery.SchemaField("announcement_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("file_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("records_extracted", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("total_amount", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pattern_detected", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("processed_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    try:
        table = bigquery.Table(table_id_parse_log, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="processed_at"
        )
        client.create_table(table, exists_ok=True)
        logger.info(f"✓ parse_logテーブル準備完了: {table_id_parse_log}")
    except Exception as e:
        logger.exception(f"✗ parse_logテーブル作成エラー")

ensure_parse_log_table()

# ===========================
# 正規表現の事前コンパイル
# ===========================
PATTERN_NUMBERED_LIST = re.compile(r'^\s*[1１]\s+名称', re.MULTILINE)
PATTERN_TABLE = re.compile(r'別表[\s\S]*?銘柄', re.IGNORECASE)
PATTERN_RETAIL = re.compile(r'個人向け[\s\S]*?変動', re.IGNORECASE)
PATTERN_TB = re.compile(r'政府短期証券|国庫短期証券|TB|FB')

# ===========================
# 金額抽出用パターン（v7_fixed7版）
# ===========================
# 注意: 将来「兆+万円」や「百万円」等の告示内容が出現する可能性がある
# 現在の実装では「兆」「億」「円」のみを許容しているが（ALLOWED_UNITS参照）、
# 新しい単位が必要になった場合は以下のパターンを追加する:
#   - 「兆+万円」: (re.compile(r'([0-9,，]+)\s*兆(?:\s*([0-9,，]+)\s*万)?円'), 'cho_man', ...)
#   - 「百万円」: (re.compile(r'([0-9,，]+)\s*百万円'), 'hyakuman', ...)
# ALLOWED_UNITS にも新しい単位を追加すること。
# このメモは将来の保守性のために残しておく。

AMOUNT_PATTERNS = [
    # 優先順位1: 項番付き発行額（丸数字対応）
    (re.compile(r'[（(]?[4４④⑷][)）]?[\s　]*発行額[\s　]*額面金額で[、,，\s]*([0-9,，]+)\s*円', re.DOTALL), 
     'yen', '項番付き発行額', 110),
    
    # 優先順位2: 項目6（丸数字対応）
    (re.compile(r'[（(]?[6６⑥⑹六][)）]?[\s　]*(?:発行価額の総額|発行額)[\s：:]*([0-9,，]+)\s*(兆|億)?円', re.DOTALL),
     'auto', '項目6', 105),
    
    # 優先順位3: 発行額
    (re.compile(r'発行額[^0-9]{0,50}([0-9,，]+)\s*(億)?円', re.DOTALL), 
     'auto', '発行額', 100),
    
    # 優先順位4: 兆+億円表記
    (re.compile(r'([0-9,，]+)\s*兆(?:\s*([0-9,，]+)\s*億)?円'),
     'cho_oku', '兆+億表記', 95),
    
    # 優先順位5: 発行価額の総額
    (re.compile(r'発行価額の総額[^0-9]{0,50}([0-9,，]+)\s*(億)?円', re.DOTALL), 
     'auto', '発行価額の総額', 90),
    
    # 優先順位6: 兆円表記
    (re.compile(r'([0-9,，]+)\s*兆円'),
     'cho', '兆円表記', 85),
    
    # 優先順位7: 億円表記
    (re.compile(r'([0-9,，]+)\s*億円'), 
     'oku', '億円表記', 80),
    
    # 優先順位8: 額面金額
    (re.compile(r'額面金額(?!100円につき)(?:は|で|:|：)?\s*([0-9,，]+)\s*(億)?円', re.DOTALL), 
     'auto', '額面金額（明示）', 70),
]

# ===========================
# NFKC正規化関数
# ===========================
def normalize_text(text: str) -> str:
    """テキストをNFKC正規化"""
    return unicodedata.normalize('NFKC', text)

# ===========================
# パターン識別関数
# ===========================
def identify_pattern_simple(text: str) -> Tuple[str, float]:
    """簡易的なパターン識別"""
    if PATTERN_NUMBERED_LIST.search(text):
        return 'NUMBERED_LIST', 0.9
    if PATTERN_TABLE.search(text):
        return 'TABLE_HORIZONTAL', 0.9
    if PATTERN_RETAIL.search(text):
        return 'RETAIL_BOND', 0.9
    if PATTERN_TB.search(text):
        return 'TB_SHORT_TERM', 0.9
    return 'UNKNOWN', 0.0

# ===========================
# 簡易パース関数（v7_fixed7版）
# ===========================
def simple_parse(text: str, announcement_id: str) -> List[Dict]:
    """
    最低限の情報を抽出する簡易パーサー（v7_fixed7版）
    
    改善点:
    - 位置バケット方式で同額・近接位置を同一視（重複防止）
    - ALLOWED_UNITSで未知単位をスキップ
    """
    items = []
    selected = {}
    
    # 政府短期証券の判定
    is_tb = bool(PATTERN_TB.search(text))
    
    for pattern, mode, pattern_name, priority in AMOUNT_PATTERNS:
        matches = pattern.finditer(text)
        for match in matches:
            try:
                # 数値部分の抽出
                raw_str = match.group(1).replace(',', '').replace('，', '')
                base = int(raw_str)
                
                # 単位の判定
                if mode == 'auto':
                    unit = match.group(2) if len(match.groups()) >= 2 else None
                    if unit == '兆':
                        if '兆' not in ALLOWED_UNITS:
                            logger.debug(f"  除外: 未許可単位 ({unit})")
                            continue
                        amount = base * 1000000000000
                    elif unit == '億':
                        if '億' not in ALLOWED_UNITS:
                            logger.debug(f"  除外: 未許可単位 ({unit})")
                            continue
                        amount = base * 100000000
                    else:
                        if '円' not in ALLOWED_UNITS:
                            logger.debug(f"  除外: 未許可単位 (円)")
                            continue
                        amount = base
                elif mode == 'cho_oku':
                    if '兆' not in ALLOWED_UNITS or '億' not in ALLOWED_UNITS:
                        logger.debug(f"  除外: 未許可単位 (兆億)")
                        continue
                    cho = int(match.group(1).replace(',', '').replace('，', ''))
                    oku_str = match.group(2) if len(match.groups()) >= 2 and match.group(2) else '0'
                    oku = int(oku_str.replace(',', '').replace('，', ''))
                    amount = cho * 1000000000000 + oku * 100000000
                elif mode == 'cho':
                    if '兆' not in ALLOWED_UNITS:
                        logger.debug(f"  除外: 未許可単位 (兆)")
                        continue
                    amount = base * 1000000000000
                elif mode == 'oku':
                    if '億' not in ALLOWED_UNITS:
                        logger.debug(f"  除外: 未許可単位 (億)")
                        continue
                    amount = base * 100000000
                elif mode == 'yen':
                    if '円' not in ALLOWED_UNITS:
                        logger.debug(f"  除外: 未許可単位 (円)")
                        continue
                    amount = base
                else:
                    continue
                
                # 最小金額チェック
                if amount < MIN_AMOUNT:
                    logger.debug(f"  除外: 金額が小さすぎる ({amount:,}円)")
                    continue
                
                # 位置バケット方式で重複防止（v7_fixed7: 改善）
                # 同額で位置が近い（±20文字）ものは同一視
                start = match.start()
                bucket = start // POSITION_BUCKET_SIZE
                key = (bucket, amount)
                
                # 優先度チェック: 既存エントリより優先度が高い場合のみ上書き
                if key in selected:
                    existing_priority = selected[key][0]
                    if priority <= existing_priority:
                        logger.debug(f"  除外: 低優先度 (既存={existing_priority}, 現在={priority})")
                        continue
                    else:
                        logger.debug(f"  上書き: 高優先度 (既存={existing_priority}, 現在={priority})")
                
                # 一意な bond_name
                unique_bond_name = f'簡易抽出_{announcement_id}_{pattern_name}_{match.start()}'
                
                # カテゴリと正規化
                bond_category = '政府短期証券' if is_tb else '未分類'
                legal_basis_normalized = bond_category
                
                # snippet_fingerprintを生成
                fingerprint = snippet_fingerprint(text, match, context=24)
                
                # dedupe_key生成（sourceを除外）
                dedupe_key = generate_dedupe_key(
                    announcement_id, 
                    amount, 
                    legal_basis_normalized,
                    fingerprint
                )
                
                # アイテムの作成
                item = {
                    'announcement_id': announcement_id,
                    'bond_name': unique_bond_name,
                    'issue_amount': amount,
                    'legal_basis': '抽出中',
                    'legal_basis_normalized': legal_basis_normalized,
                    'legal_basis_source': f'simple_parse_v7_fixed7_{pattern_name}',
                    'bond_category': bond_category,
                    'mof_category': bond_category,
                    'data_quality_score': priority,
                    'is_summary_record': False,
                    'is_detail_record': True,
                    'dedupe_key': dedupe_key,
                }
                
                # 記録
                selected[key] = (priority, pattern_name, item)
                logger.debug(f"  抽出成功: {amount/100000000:.2f}億円 [パターン: {pattern_name}, 優先度: {priority}, バケット: {bucket}]")
                
            except Exception as e:
                logger.debug(f"  パース警告: {e}")
                continue
    
    # 最終的なアイテムリスト
    items = [item for priority, pattern_name, item in sorted(selected.values(), key=lambda x: x[0], reverse=True)]
    
    return items

# ===========================
# parse_logへの記録
# ===========================
def log_parse_result(announcement_id: str, file_name: str, status: str, error_message: str = None,
                     records_extracted: int = 0, total_amount: int = 0,
                     pattern_detected: str = None):
    """parse_logテーブルに処理結果を記録"""
    try:
        log_entry = {
            'announcement_id': announcement_id,
            'file_name': file_name,
            'status': status,
            'error_message': error_message,
            'records_extracted': records_extracted,
            'total_amount': total_amount,
            'pattern_detected': pattern_detected,
            'processed_at': now_rfc3339(),
        }
        client.insert_rows_json(table_id_parse_log, [log_entry])
    except Exception as e:
        logger.exception(f"  ⚠ parse_log記録エラー")

# ===========================
# MERGE文による投入（v7_fixed7版）
# ===========================
def merge_to_layer2(items: List[Dict]) -> Tuple[bool, str]:
    """
    ステージングテーブル経由のMERGE実装（v7_fixed7版）
    
    改善点:
    - dmlStats.insertedRowCountから挿入件数を取得（並行実行対応）
    - フォールバック処理を追加（将来変更対応）
    - リトライ時に指数バックオフを使用
    
    Returns:
        (success: bool, status: str)
        status: 'SUCCESS', 'NOOP_DUPLICATES', 'FAILURE'
    """
    if not items:
        return False, 'FAILURE'
    
    staging_table = None
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            # ステップ1: 一時ステージングテーブル名
            staging_table = f"{table_id_layer2}__stg_{uuid4().hex[:8]}"
            logger.debug(f"  ステージングテーブル: {staging_table}")
            
            # ステップ2: スキーマ
            schema = [
                bigquery.SchemaField("announcement_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("bond_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("issue_amount", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("legal_basis", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("legal_basis_normalized", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("legal_basis_source", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("bond_category", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("mof_category", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("data_quality_score", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("is_summary_record", "BOOL", mode="NULLABLE"),
                bigquery.SchemaField("is_detail_record", "BOOL", mode="NULLABLE"),
                bigquery.SchemaField("dedupe_key", "STRING", mode="REQUIRED"),
            ]
            
            # テーブル作成（有効期限付き）
            table = bigquery.Table(staging_table, schema=schema)
            table.expires = datetime.now(timezone.utc) + timedelta(days=1)
            client.create_table(table, exists_ok=True)
            logger.debug(f"  ✓ ステージングテーブル作成完了（有効期限: 1日）")
            
            # ステップ3: 型を確定させてからロード
            def cast_row(r: dict) -> dict:
                """dictを正しい型に変換"""
                return {
                    "announcement_id": str(r["announcement_id"]),
                    "bond_name": str(r["bond_name"]),
                    "issue_amount": int(r["issue_amount"]),
                    "legal_basis": r.get("legal_basis") or r.get("legal_basis_normalized") or None,
                    "legal_basis_normalized": r.get("legal_basis_normalized") or None,
                    "legal_basis_source": r.get("legal_basis_source") or None,
                    "bond_category": r.get("bond_category") or None,
                    "mof_category": r.get("mof_category") or None,
                    "data_quality_score": int(r.get("data_quality_score", 0)),
                    "is_summary_record": bool(r.get("is_summary_record", False)),
                    "is_detail_record": bool(r.get("is_detail_record", True)),
                    "dedupe_key": r.get("dedupe_key") or None,
                }
            
            # バリデーション
            def validate_row(r: dict, idx: int):
                """行のバリデーション"""
                assert r["announcement_id"], f"row[{idx}]: announcement_id must be non-empty"
                assert r["bond_name"], f"row[{idx}]: bond_name must be non-empty"
                assert isinstance(r["issue_amount"], int), f"row[{idx}]: issue_amount must be int"
                assert r["issue_amount"] > 0, f"row[{idx}]: issue_amount must be positive"
                assert r["dedupe_key"], f"row[{idx}]: dedupe_key must be non-empty"
            
            # 変換とバリデーション
            rows = []
            for i, item in enumerate(items):
                row = cast_row(item)
                validate_row(row, i)
                rows.append(row)
            
            logger.debug(f"  ✓ 行の変換・バリデーション完了: {len(rows)}件")
            
            # load_table_from_jsonでロード
            load_cfg = LoadJobConfig(
                schema=schema,
                write_disposition=WriteDisposition.WRITE_TRUNCATE
            )
            load_job = client.load_table_from_json(rows, staging_table, job_config=load_cfg, location=LOCATION)
            load_job.result()
            
            logger.debug(f"  ✓ ステージングテーブルへのロード完了")
            
            # ステップ4: MERGE実行
            merge_sql = f"""
            MERGE `{table_id_layer2}` T
            USING `{staging_table}` S
            ON T.dedupe_key = S.dedupe_key
            WHEN NOT MATCHED THEN
              INSERT ROW
            """
            
            merge_job = client.query(merge_sql, location=LOCATION)
            result = merge_job.result()
            
            # ステップ5: dmlStatsから挿入件数を取得（並行実行対応）
            ins_count = int(merge_job._properties
                           .get('statistics', {})
                           .get('query', {})
                           .get('dmlStats', {})
                           .get('insertedRowCount', 0))
            
            # フォールバック: dmlStatsが取得できない場合（将来変更対応）
            if not ins_count and getattr(merge_job, 'num_dml_affected_rows', None):
                # MERGEでは厳密には affected=insert+update+delete だが 0/非0 の判定に使える
                ins_count = merge_job.num_dml_affected_rows or 0
                logger.debug(f"  ℹ dmlStatsフォールバック使用: {ins_count}件")
            
            logger.debug(f"  ✓ MERGE完了: {len(items)}件（staging経由）")
            
            # ステップ6: 挿入件数に応じてステータスを決定
            if ins_count == 0:
                logger.info(f"  ℹ 全て重複: {len(items)}件")
                return True, 'NOOP_DUPLICATES'
            else:
                logger.debug(f"  ✓ 新規挿入: {ins_count}件")
                return True, 'SUCCESS'
            
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"  ⚠ MERGE失敗（試行{attempt+1}回目）: {e}")
                exponential_backoff_sleep(attempt)  # v7_fixed7: 指数バックオフ
                continue
            else:
                logger.exception(f"  ✗ MERGE最終失敗（{max_retries}回試行）")
                return False, 'FAILURE'
            
        finally:
            # ステップ7: ステージングテーブルの掃除
            if staging_table:
                try:
                    client.delete_table(staging_table, not_found_ok=True)
                    logger.debug(f"  ✓ ステージングテーブル削除完了")
                except Exception as e:
                    logger.warning(f"  ⚠ ステージングテーブル削除エラー: {e}")
    
    return False, 'FAILURE'

# ===========================
# メイン処理
# ===========================
logger.info("")
logger.info("=" * 80)
logger.info("処理開始")
logger.info("=" * 80)
logger.info("")

# 処理カウンター
success_count = 0
noop_count = 0
failure_count = 0
skip_count = 0
total_records = 0
total_amount = 0

for i, file_path in enumerate(test_files, 1):
    announcement_id = file_path.stem
    file_name = file_path.name
    
    logger.info(f"[{i}/{len(test_files)}] {announcement_id}")
    
    try:
        # ファイル読み込み
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_text = f.read()
        
        logger.info(f"  ✓ ファイル読み込み: {len(raw_text)}文字")
        
        # NFKC正規化
        normalized_text = normalize_text(raw_text)
        logger.debug(f"  ✓ NFKC正規化完了")
        
        # パターン識別
        pattern, confidence = identify_pattern_simple(normalized_text)
        logger.info(f"  ✓ パターン: {pattern} (信頼度: {confidence:.2f})")
        
        # 簡易パース
        items = simple_parse(normalized_text, announcement_id)
        
        if not items:
            logger.warning(f"  ⚠ データ抽出失敗")
            log_parse_result(announcement_id, file_name, 'FAILURE', 
                           error_message='No data extracted',
                           pattern_detected=pattern)
            failure_count += 1
            continue
        
        logger.info(f"  ✓ データ抽出: {len(items)}件")
        
        # 合計金額の計算
        batch_total = sum(item['issue_amount'] for item in items)
        logger.info(f"  ✓ 合計金額: {batch_total / 100000000:.2f}億円")
        
        # Layer2へMERGE
        success, status = merge_to_layer2(items)
        
        if success:
            if status == 'SUCCESS':
                logger.info(f"  ✓ 成功: Layer2へMERGE完了")
                success_count += 1
                total_records += len(items)
                total_amount += batch_total
            elif status == 'NOOP_DUPLICATES':
                logger.info(f"  ✓ 重複: 全て既存データ")
                noop_count += 1
            
            log_parse_result(announcement_id, file_name, status,
                           records_extracted=len(items),
                           total_amount=batch_total,
                           pattern_detected=pattern)
        else:
            logger.error(f"  ✗ MERGE失敗")
            log_parse_result(announcement_id, file_name, 'FAILURE',
                           error_message='MERGE failed',
                           records_extracted=len(items),
                           total_amount=batch_total,
                           pattern_detected=pattern)
            failure_count += 1
        
    except Exception as e:
        logger.exception(f"  ✗ 処理エラー")
        log_parse_result(announcement_id, file_name, 'FAILURE', error_message=str(e))
        failure_count += 1

# ===========================
# 結果サマリー
# ===========================
logger.info("")
logger.info("=" * 80)
logger.info("処理結果")
logger.info("=" * 80)
logger.info(f"総ファイル数: {len(test_files)}件")
logger.info(f"成功: {success_count}件")
logger.info(f"重複: {noop_count}件")
logger.info(f"失敗: {failure_count}件")
logger.info(f"スキップ: {skip_count}件")
if len(test_files) > 0:
    logger.info(f"成功率: {(success_count + noop_count) / len(test_files) * 100:.1f}%")
logger.info(f"総レコード数: {total_records}件")
logger.info(f"総発行額: {total_amount / 1000000000000:.2f}兆円")
logger.info("=" * 80)

# ===========================
# Layer2確認（重複チェック付き + カテゴリ直接表示）
# ===========================
try:
    count_query = f"SELECT COUNT(*) as cnt FROM `{table_id_layer2}`"
    result = list(client.query(count_query, location=LOCATION).result())[0]
    logger.info(f"\nLayer2 総レコード数: {result.cnt}件")
    
    # 重複チェック
    dup_query = f"""
    SELECT 
        dedupe_key,
        COUNT(*) as cnt,
        STRING_AGG(announcement_id, ', ') as announcement_ids
    FROM `{table_id_layer2}`
    GROUP BY dedupe_key
    HAVING cnt > 1
    """
    dup_results = list(client.query(dup_query, location=LOCATION).result())
    
    if dup_results:
        logger.warning(f"\n⚠ 重複検出: {len(dup_results)}件")
        for row in dup_results[:5]:
            logger.warning(f"  dedupe_key: {row.dedupe_key[:16]}... × {row.cnt}回 ({row.announcement_ids})")
    else:
        logger.info("\n✓ 重複なし")
    
    if result.cnt > 0:
        # カテゴリ直接表示（v7_fixed7版: CASEを削除）
        sample_query = f"""
        SELECT 
            announcement_id, 
            bond_name, 
            issue_amount, 
            bond_category AS bond_category_display,
            LEFT(dedupe_key, 16) as dedupe_key_prefix
        FROM `{table_id_layer2}`
        ORDER BY issue_amount DESC
        LIMIT 5
        """
        logger.info("\nサンプルレコード（金額上位5件）:")
        results = client.query(sample_query, location=LOCATION).result()
        for row in results:
            logger.info(f"  {row.announcement_id}: {row.issue_amount:,}円 ({row.issue_amount/100000000:.2f}億円) [{row.bond_category_display}]")
            logger.debug(f"    dedupe_key: {row.dedupe_key_prefix}...")
except Exception as e:
    logger.exception(f"検証エラー")

logger.info("")
logger.info("=" * 80)
logger.info("処理完了")
logger.info("=" * 80)
logger.info(f"詳細ログ: {args.log_file}")
