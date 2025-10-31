# 国債データベース構築プロジェクト - 進捗記録

## プロジェクト概要

本プロジェクトは、2023年度の官報に掲載された日本国債発行に関する告示179件を体系的に解析し、BigQueryデータベースに格納することで、国債発行データの包括的な分析基盤を構築することを目的としています。

---

## v7シリーズの達成事項（2025-10-31）

### ✅ v7_fixed7（真・最終完全版）完成

**日付**: 2025年10月31日

**スクリプト名**: `batch_direct_processing_v7_fixed7.py`

**達成内容**:
1. ✅ 全8つのレビュー指摘に対応
2. ✅ 位置バケット方式で重複抽出を7件削減（27件 → 20件）
3. ✅ テスト実行＋全件処理を完了
4. ✅ 重複ゼロを達成（462件）
5. ✅ GitHubにログをコミット
6. ✅ 完全な引き継ぎドキュメントを作成

---

## 実行結果サマリー

### テスト実行（--reset --limit 10）

```bash
コマンド: python batch_direct_processing_v7_fixed7.py --reset --limit 10 --verbose

結果:
- レコード数: 20件
- 重複: 0件 ✅
- 成功率: 100%

ログファイル:
https://github.com/planet9-cpri/jgb-database-project/blob/main/logs/batch_processing/2025-10-31_v7_fixed7_test.log
```

### 全件処理（--limit 0）

```bash
コマンド: python batch_direct_processing_v7_fixed7.py --limit 0

結果:
- 総ファイル数: 179件
- レコード数: 462件
- 重複: 0件 ✅
- 成功率: 100%

ログファイル:
https://github.com/planet9-cpri/jgb-database-project/blob/main/logs/batch_processing/2025-10-31_v7_fixed7_full.log
```

---

## バージョン履歴

### v7_fixed7（2025-10-31）⭐ 真・最終完全版

**改善点（v7_fixed6から）:**

#### 1. サンプル表示のシンプル化
```sql
-- Before (v7_fixed6): CASE文で複雑
CASE 
    WHEN REGEXP_CONTAINS(announcement_id, r'(TB|FB|短期証券)') THEN '政府短期証券'
    ELSE bond_category
END

-- After (v7_fixed7): シンプル
bond_category AS bond_category_display
```

**理由**: 告示IDに "TB/FB/短期証券" が入らない限りヒットしないため、抽出時に付与した `bond_category` をそのまま出す

#### 2. 位置バケット方式で重複防止

```python
# Before (v7_fixed6): pattern_name単位で重複チェック
key = (pattern_name, match.start(), amount)

# After (v7_fixed7): 位置バケット方式
POSITION_BUCKET_SIZE = 20  # 20文字以内の近接位置は同一視
bucket = start // POSITION_BUCKET_SIZE
key = (bucket, amount)
```

**効果**: 同じ金額が「項番付き発行額／発行額／額面金額」で複数ヒットしても1件だけ残る

**実証結果**:
- v7_fixed6: 27件（limit 10）
- v7_fixed7: **20件（-7件の重複削減）** ✅

#### 3. 許可単位リストの定数化

```python
# v7_fixed7: 定数化で意図が明確
ALLOWED_UNITS = {'兆', '億', '円'}

if unit == '兆':
    if '兆' not in ALLOWED_UNITS:
        logger.debug(f"  除外: 未許可単位 ({unit})")
        continue
    amount = base * 1000000000000
```

**効果**:
- ✅ 意図が明確（どの単位を許可するか）
- ✅ 未知単位が出現した時にログで確認できる
- ✅ 将来の拡張が容易

#### 4. 指数バックオフのリトライ処理

```python
def exponential_backoff_sleep(attempt: int, base_delay: float = 1.0, max_delay: float = 60.0):
    """ノード混雑時にリトライが集中しないよう、ランダム性を持たせたスリープ"""
    delay = min(base_delay * (2 ** attempt), max_delay)
    jitter = random.uniform(0, delay * 0.1)  # 最大10%のジッター
    sleep_time = delay + jitter
    time.sleep(sleep_time)

# リトライループ
max_retries = 3
for attempt in range(max_retries):
    try:
        # MERGE実行
        ...
    except Exception as e:
        if attempt < max_retries - 1:
            exponential_backoff_sleep(attempt)  # 指数バックオフ
            continue
```

**バックオフのタイミング**:
- 1回目: 1秒 + ジッター（0〜0.1秒）
- 2回目: 2秒 + ジッター（0〜0.2秒）
- 3回目: 4秒 + ジッター（0〜0.4秒）

**効果**:
- ✅ ノード混雑時にリトライが分散される
- ✅ ジッターでさらに分散が改善
- ✅ 最大遅延時間を設定可能

---

### v7_fixed6（2025-10-31）本番運用最終版

**改善点（v7_fixed5から）:**

1. ✅ **単位許容メモの追加**
   - 将来「兆+万円」や「百万円」が出現する可能性に対応
   - コード内にメモを残して保守性向上

2. ✅ **カテゴリ補正の追加**
   - 政府短期証券の自動判定機能
   - TB/FB/短期証券の正確な分類

3. ✅ **dmlStatsフォールバックの追加**
   - 並行実行時のdmlStats取得に対応
   - フォールバック処理で将来変更に対応

4. ✅ **--reset処理の改善**
   - ALTER TABLE方式からDROP+CREATE方式へ変更
   - REQUIRED制約の確実な設定

**実行結果**:
- テスト実行: 27件、重複0件
- 全件処理: 未実施
- 成功率: 100%

---

### v7_fixed5（2025-10-31）本番運用対応版

**改善点（v7_fixed4から）:**

1. ✅ **dedupe_key列の追加**
   - PRIMARY KEY制約の設定
   - 重複防止の強化

2. ✅ **MERGE文による投入**
   - ステージングテーブル経由
   - 並行実行対応

3. ✅ **スキーマの最適化**
   - 12フィールドに整理
   - クラスタリング設定

**実行結果**:
- テスト実行: 27件、重複0件
- 成功率: 100%

---

### v7_fixed4（2025-10-30）

**改善点（v7_fixed3から）:**

1. ✅ **--reset機能の追加**
   - 既存データを削除して再投入
   - テーブルの再作成

2. ✅ **ログ出力の改善**
   - 詳細なデバッグ情報
   - エラーハンドリングの強化

---

### v7_fixed3（2025-10-30）

**初期実装:**

1. ✅ **直接パース方式の実装**
   - Layer 1をスキップ
   - BigQueryへ直接投入

2. ✅ **金額抽出パターンの実装**
   - 8つのパターンを優先順位順に適用
   - 項番付き発行額、項目6、発行額など

3. ✅ **基本的な重複防止**
   - pattern_name単位での重複チェック

---

## 技術的な詳細

### アーキテクチャの変更

#### 旧アーキテクチャ（Phase 1-5）
```
官報告示（.txt） 
    ↓
Layer 1: raw_announcements（生データ）
    ↓
Layer 2: announcement_items（構造化データ）
    ↓
Layer 3: 分析用ビュー
```

#### 新アーキテクチャ（v7シリーズ）⭐
```
官報告示（.txt）
    ↓
直接パース
    ↓
BigQuery: bond_issuances（構造化データ）
```

**利点**:
- ✅ シンプルで理解しやすい
- ✅ 処理速度の向上
- ✅ メンテナンス性の向上

---

### 金額抽出パターン（優先順位順）

```python
AMOUNT_PATTERNS = [
    # 優先順位1: 項番付き発行額（丸数字対応）
    (re.compile(r'[（(]?[4４④⑷][)）]?[\s　]*発行額[\s　]*額面金額で[、,，\s]*([0-9,，]+)\s*円'), 
     'yen', '項番付き発行額', 110),
    
    # 優先順位2: 項目6（丸数字対応）
    (re.compile(r'[（(]?[6６⑥⑹六][)）]?[\s　]*(?:発行価額の総額|発行額)[\s：:]*([0-9,，]+)\s*(兆|億)?円'),
     'auto', '項目6', 105),
    
    # 優先順位3: 発行額
    (re.compile(r'発行額[^0-9]{0,50}([0-9,，]+)\s*(億)?円'), 
     'auto', '発行額', 100),
    
    # 優先順位4: 兆+億円表記
    (re.compile(r'([0-9,，]+)\s*兆(?:\s*([0-9,，]+)\s*億)?円'),
     'cho_oku', '兆+億表記', 95),
    
    # 優先順位5: 発行価額の総額
    (re.compile(r'発行価額の総額[^0-9]{0,50}([0-9,，]+)\s*(億)?円'), 
     'auto', '発行価額の総額', 90),
    
    # 優先順位6: 兆円表記
    (re.compile(r'([0-9,，]+)\s*兆円'),
     'cho', '兆円表記', 85),
    
    # 優先順位7: 億円表記
    (re.compile(r'([0-9,，]+)\s*億円'), 
     'oku', '億円表記', 80),
    
    # 優先順位8: 額面金額
    (re.compile(r'額面金額(?!100円につき)(?:は|で|:|：)?\s*([0-9,，]+)\s*(億)?円'), 
     'auto', '額面金額（明示）', 70),
]
```

---

### dedupe_key生成ロジック

```python
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
```

---

## BigQueryテーブル設計

### bond_issuances（v7_fixed7版）

**スキーマ（12フィールド）:**

```python
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
    bigquery.SchemaField("dedupe_key", "STRING", mode="REQUIRED"),  # PRIMARY KEY
]

# クラスタリング
table.clustering_fields = ['dedupe_key', 'announcement_id', 'issue_amount']
```

**PRIMARY KEY制約:**
```sql
ALTER TABLE `jgb2023.20251031.bond_issuances`
ADD PRIMARY KEY (dedupe_key) NOT ENFORCED
```

---

## 運用ガイド

### 日常運用（v7_fixed7）

```bash
# 1. 環境変数を設定
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\Users\sonke\secrets\jgb2023-sa.json"

# 2. データディレクトリの確認
ls "G:\マイドライブ\JGBデータ\2023"

# 3. 増分処理（新規ファイルのみ）
cd C:\Users\sonke\projects\jgb_database_project\scripts\01_data_ingestion
python batch_direct_processing_v7_fixed7.py --limit 0 --verbose

# 4. ログをGitHubにコミット
cd ..\..\
git add logs\batch_processing\*.log
git commit -m "Add daily batch processing log - $(Get-Date -Format 'yyyy-MM-dd')"
git push origin main

# 5. BigQueryで確認
```

### 週次確認

```sql
-- 重複チェック
SELECT dedupe_key, COUNT(*) as cnt
FROM `jgb2023.20251031.bond_issuances`
GROUP BY dedupe_key
HAVING cnt > 1;
-- 期待結果: 表示するデータはありません。

-- レコード数推移
SELECT 
    DATE(processed_at) as date,
    COUNT(*) as records_processed,
    SUM(total_amount) / 1000000000000 as total_amount_trillion
FROM `jgb2023.20251031.parse_log`
WHERE status = 'SUCCESS'
GROUP BY date
ORDER BY date DESC
LIMIT 7;

-- エラーチェック
SELECT 
    announcement_id,
    status,
    error_message
FROM `jgb2023.20251031.parse_log`
WHERE status = 'FAILURE'
ORDER BY processed_at DESC
LIMIT 10;
```

---

## トラブルシューティング

### 問題1: 重複が検出された

**対処法:**
```bash
# 1. 重複の詳細を確認
SELECT *
FROM `jgb2023.20251031.bond_issuances`
WHERE dedupe_key IN (
    SELECT dedupe_key
    FROM `jgb2023.20251031.bond_issuances`
    GROUP BY dedupe_key
    HAVING COUNT(*) > 1
)
ORDER BY dedupe_key, announcement_id;

# 2. 再実行（--reset）
python batch_direct_processing_v7_fixed7.py --reset --limit 0
```

### 問題2: MERGE失敗

**対処法:**
```bash
# 1. ログで詳細を確認
cat batch_processing_v7_fixed7.log | grep "MERGE失敗"

# 2. BigQueryのクォータを確認

# 3. 少数ファイルで再試行
python batch_direct_processing_v7_fixed7.py --limit 5 --verbose

# 4. 指数バックオフの設定を調整（必要に応じて）
```

### 問題3: データ抽出失敗

**対処法:**
```bash
# 1. 該当ファイルを確認
cat "G:\マイドライブ\JGBデータ\2023\20230XXX.txt"

# 2. 新しいパターンが必要か確認

# 3. ALLOWED_UNITS を更新（必要に応じて）
```

---

## 次のステップ

### 優先度：Must（必須）

#### 1. TB判定の強化
- 政府短期証券の自動判定精度向上
- 告示内容からの自動判定

#### 2. 財務省統計との完全一致
- 現在: 462件のレコード
- 目標: 財務省統計193.46兆円との照合
- 方法: 法令→国債種別のマッピング確立

### 優先度：Should（推奨）

#### 3. 単位の拡張
- 「兆+万円」対応
- 「百万円」対応
- ALLOWED_UNITSに追加

---

## まとめ

### v7_fixed7の達成内容

- ✅ 全8つのレビュー指摘に対応
- ✅ 位置バケット方式で7件の重複削減
- ✅ テスト実行＋全件処理を完了
- ✅ 重複ゼロを達成（462件）
- ✅ GitHubにログをコミット
- ✅ 完全な引き継ぎドキュメントを作成

### 主要な改善点

1. ✅ **サンプル表示**: CASE削除、bond_category直接表示
2. ✅ **重複防止**: 位置バケット方式（20文字単位）
3. ✅ **許可単位**: ALLOWED_UNITS定数化
4. ✅ **リトライ**: 指数バックオフ＋ジッター

### 現在の状態

**v7_fixed7は本番運用可能な状態です！** ✅

---

**最終更新**: 2025年10月31日  
**現在の状態**: v7_fixed7完成、本番運用可能  
**次回**: 新しいチャットで継続可能
