# 📋 v7_fixed7 引き継ぎドキュメント

## ✅ 実行結果サマリー（2025-10-31）

### テスト実行（--reset --limit 10）

```
実行コマンド:
python batch_direct_processing_v7_fixed7.py --reset --limit 10 --verbose

結果:
- レコード数: 20件
- 重複: 0件
- 成功率: 100%

ログファイル:
https://github.com/planet9-cpri/jgb-database-project/blob/main/logs/batch_processing/2025-10-31_v7_fixed7_test.log
```

### 全件処理（--limit 0）

```
実行コマンド:
python batch_direct_processing_v7_fixed7.py --limit 0

結果:
- 総ファイル数: 179件
- レコード数: 462件
- 重複: 0件
- 成功率: 100%

ログファイル:
https://github.com/planet9-cpri/jgb-database-project/blob/main/logs/batch_processing/2025-10-31_v7_fixed7_full.log
```

---

## 📊 バージョン比較

### レコード数の推移（limit 10での比較）

| バージョン | レコード数 | 重複 | 備考 |
|-----------|-----------|------|------|
| v7_fixed5 | 27件 | 0件 | 初回テスト |
| v7_fixed6 | 27件 | 0件 | 4つの改善 |
| v7_fixed7 | **20件** | 0件 | **位置バケット方式で7件削減** ✅ |

**重要な発見:**
- v7_fixed7で **7件の重複抽出を防止**（27件 → 20件）
- 位置バケット方式が効果的に機能
- 同じ金額が近い位置で複数ヒットする問題を解決

### 全件処理の結果

```
総ファイル数: 179件
総レコード数: 462件
平均: 2.58件/ファイル

重複: 0件 ✅
```

---

## 🎯 最終的なシステム構成

### 1. スクリプト

**最新版:** `batch_direct_processing_v7_fixed7.py`

**保存場所:**
```
C:\Users\sonke\projects\jgb_database_project\scripts\01_data_ingestion\batch_direct_processing_v7_fixed7.py
```

**GitHub:**
```
https://github.com/planet9-cpri/jgb-database-project/blob/main/scripts/01_data_ingestion/batch_direct_processing_v7_fixed7.py
```

### 2. BigQueryテーブル

**プロジェクト:** `jgb2023`  
**データセット:** `20251031`  
**テーブル:** `bond_issuances`

**スキーマ:**
- `dedupe_key` (STRING, REQUIRED) - PRIMARY KEY
- `announcement_id` (STRING, REQUIRED)
- `bond_name` (STRING, REQUIRED)
- `issue_amount` (INTEGER, REQUIRED)
- `legal_basis` (STRING, NULLABLE)
- `legal_basis_normalized` (STRING, NULLABLE)
- `legal_basis_source` (STRING, NULLABLE)
- `bond_category` (STRING, NULLABLE)
- `mof_category` (STRING, NULLABLE)
- `data_quality_score` (INTEGER, NULLABLE)
- `is_summary_record` (BOOLEAN, NULLABLE)
- `is_detail_record` (BOOLEAN, NULLABLE)

**クラスタリング:** `dedupe_key`, `announcement_id`, `issue_amount`

### 3. ログ管理

**ログディレクトリ:**
```
C:\Users\sonke\projects\jgb_database_project\logs\batch_processing\
```

**GitHub:**
```
https://github.com/planet9-cpri/jgb-database-project/tree/main/logs/batch_processing
```

**命名規則:**
```
YYYY-MM-DD_v7_fixed7_<type>.log

例:
- 2025-10-31_v7_fixed7_test.log (テスト実行)
- 2025-10-31_v7_fixed7_full.log (全件処理)
```

---

## 🚀 日常運用ガイド

### 日次処理（推奨）

```bash
# 1. 環境変数を設定
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\Users\sonke\secrets\jgb2023-sa.json"

# 2. データディレクトリの確認
ls "G:\マイドライブ\JGBデータ\2023"

# 3. 増分処理（新規ファイルのみ）
cd C:\Users\sonke\projects\jgb_database_project\scripts\01_data_ingestion
python batch_direct_processing_v7_fixed7.py --limit 0 --verbose `
  --log-file ..\..\logs\batch_processing\$(Get-Date -Format 'yyyy-MM-dd')_v7_fixed7_daily.log

# 4. ログをGitHubにコミット
cd ..\..\
git add logs\batch_processing\$(Get-Date -Format 'yyyy-MM-dd')_v7_fixed7_daily.log
git commit -m "Add daily batch processing log - $(Get-Date -Format 'yyyy-MM-dd')"
git push origin main

# 5. BigQueryで確認
# - レコード数
# - 重複チェック
```

### 週次確認（推奨）

```bash
# 1. 重複チェック
# BigQueryで以下のクエリを実行

SELECT 
    dedupe_key,
    COUNT(*) as cnt,
    STRING_AGG(announcement_id, ', ') as announcement_ids
FROM `jgb2023.20251031.bond_issuances`
GROUP BY dedupe_key
HAVING cnt > 1;

# 期待結果: 表示するデータはありません。

# 2. レコード数推移の確認
SELECT 
    DATE(processed_at) as date,
    COUNT(*) as records_processed,
    SUM(total_amount) / 1000000000000 as total_amount_trillion
FROM `jgb2023.20251031.parse_log`
WHERE status = 'SUCCESS'
GROUP BY date
ORDER BY date DESC
LIMIT 7;

# 3. エラーチェック
SELECT 
    announcement_id,
    status,
    error_message
FROM `jgb2023.20251031.parse_log`
WHERE status = 'FAILURE'
ORDER BY processed_at DESC
LIMIT 10;
```

### 月次メンテナンス（推奨）

```bash
# 1. データセットの確認
# BigQueryコンソールで以下を確認:
# - テーブルサイズ
# - パーティション数
# - クラスタリング効率

# 2. ログのアーカイブ
cd C:\Users\sonke\projects\jgb_database_project\logs\batch_processing

# 先月のログを圧縮
$lastMonth = (Get-Date).AddMonths(-1).ToString('yyyy-MM')
tar -czf archive_${lastMonth}.tar.gz ${lastMonth}*.log
git add archive_${lastMonth}.tar.gz
git commit -m "Archive logs for ${lastMonth}"
git push origin main

# 3. データベースのバックアップ
# BigQueryコンソールで:
# - テーブルをエクスポート（GCS経由）
# - スナップショットを作成
```

---

## 🔧 トラブルシューティング

### 問題1: 重複が検出された

**症状:**
```sql
SELECT dedupe_key, COUNT(*) as cnt
FROM `jgb2023.20251031.bond_issuances`
GROUP BY dedupe_key
HAVING cnt > 1;

-- 結果: 1件以上の重複
```

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

# 2. 重複を削除（手動）
DELETE FROM `jgb2023.20251031.bond_issuances`
WHERE ROWID NOT IN (
    SELECT MIN(ROWID)
    FROM `jgb2023.20251031.bond_issuances`
    GROUP BY dedupe_key
);

# 3. 再実行
python batch_direct_processing_v7_fixed7.py --reset --limit 0
```

### 問題2: MERGE失敗

**症状:**
```
✗ MERGE最終失敗（3回試行）
```

**対処法:**
```bash
# 1. ログで詳細を確認
cat batch_processing_v7_fixed7.log | grep "MERGE失敗"

# 2. BigQueryのクォータを確認
# - API呼び出し制限
# - ストレージ制限
# - 同時実行制限

# 3. 少数ファイルで再試行
python batch_direct_processing_v7_fixed7.py --limit 5 --verbose

# 4. 解決しない場合は指数バックオフの設定を調整
# batch_direct_processing_v7_fixed7.py の以下の行を編集:
# base_delay: float = 1.0 → 2.0 に変更
# max_delay: float = 60.0 → 120.0 に変更
```

### 問題3: データ抽出失敗

**症状:**
```
⚠ データ抽出失敗
```

**対処法:**
```bash
# 1. 該当ファイルを確認
cat "G:\マイドライブ\JGBデータ\2023\20230XXX.txt"

# 2. 新しいパターンが必要か確認
# - 「兆+万円」等の新単位
# - 新しいフォーマット

# 3. ALLOWED_UNITS を更新（必要に応じて）
# batch_direct_processing_v7_fixed7.py の以下の行を編集:
# ALLOWED_UNITS = {'兆', '億', '円', '万'}  # '万' を追加
```

### 問題4: クラスタリングが効いていない

**症状:**
```sql
SELECT clustering_fields
FROM `jgb2023.20251031.__TABLES__`
WHERE table_name = 'bond_issuances';

-- 結果: NULL
```

**対処法:**
```bash
# 1. --reset で再作成
python batch_direct_processing_v7_fixed7.py --reset --limit 0

# 2. クラスタリングを手動で設定
ALTER TABLE `jgb2023.20251031.bond_issuances`
CLUSTER BY dedupe_key, announcement_id, issue_amount;
```

---

## 📈 パフォーマンスモニタリング

### 主要指標

```sql
-- 1. 処理成功率
SELECT 
    DATE(processed_at) as date,
    COUNT(*) as total,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success,
    ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
FROM `jgb2023.20251031.parse_log`
GROUP BY date
ORDER BY date DESC;

-- 2. 平均処理時間（ログから手動で確認）
-- 各ファイルの処理時間をログで確認

-- 3. データ品質スコア分布
SELECT 
    data_quality_score,
    COUNT(*) as cnt
FROM `jgb2023.20251031.bond_issuances`
GROUP BY data_quality_score
ORDER BY data_quality_score DESC;

-- 4. カテゴリ分布
SELECT 
    bond_category,
    COUNT(*) as cnt,
    SUM(issue_amount) / 1000000000000 as total_amount_trillion
FROM `jgb2023.20251031.bond_issuances`
GROUP BY bond_category
ORDER BY cnt DESC;
```

---

## 🔒 セキュリティとバックアップ

### 認証情報の管理

```bash
# 1. サービスアカウントキーの保存場所
C:\Users\sonke\secrets\jgb2023-sa.json

# 2. 権限の確認
# GCPコンソールで確認:
# - BigQuery データ編集者
# - BigQuery ジョブユーザー

# 3. 定期的なローテーション（推奨: 90日ごと）
# GCPコンソールでキーを再生成
```

### バックアップ戦略

```bash
# 1. BigQueryテーブルのエクスポート（月次推奨）
# BigQueryコンソールで:
# bond_issuances → エクスポート → GCS
# フォーマット: Avro（推奨）または JSON

# 2. GitHubでのコード管理
git remote -v
# origin  https://github.com/planet9-cpri/jgb-database-project.git

# 定期的にプッシュ
git push origin main

# 3. ログのアーカイブ
# 月次でログを圧縮・保存
```

---

## 📚 関連ドキュメント

### GitHub上のドキュメント

```
https://github.com/planet9-cpri/jgb-database-project/tree/main/docs/
```

### 重要なファイル

1. **メインスクリプト**
   - `scripts/01_data_ingestion/batch_direct_processing_v7_fixed7.py`

2. **ログ**
   - `logs/batch_processing/2025-10-31_v7_fixed7_test.log`
   - `logs/batch_processing/2025-10-31_v7_fixed7_full.log`

3. **ドキュメント**
   - `docs/V7_FIXED7_FINAL_PRESENTATION.md`
   - `docs/V7_FIXED7_COMPLETE_GUIDE.md`
   - `docs/GITHUB_LOG_WORKFLOW.md`

---

## 🎓 新しい担当者へのオンボーディング

### ステップ1: 環境セットアップ（所要時間: 30分）

```bash
# 1. Gitリポジトリのクローン
git clone https://github.com/planet9-cpri/jgb-database-project.git
cd jgb-database-project

# 2. Python環境のセットアップ
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install google-cloud-bigquery

# 3. 認証情報の設定
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\Users\sonke\secrets\jgb2023-sa.json"

# 4. テスト実行
cd scripts\01_data_ingestion
python batch_direct_processing_v7_fixed7.py --limit 5 --verbose
```

### ステップ2: ドキュメントの確認（所要時間: 1時間）

1. [V7_FIXED7_FINAL_PRESENTATION.md](https://github.com/planet9-cpri/jgb-database-project/blob/main/docs/V7_FIXED7_FINAL_PRESENTATION.md) を読む
2. [GITHUB_LOG_WORKFLOW.md](https://github.com/planet9-cpri/jgb-database-project/blob/main/docs/GITHUB_LOG_WORKFLOW.md) を読む
3. ログファイルを確認

### ステップ3: 実際の運用（所要時間: 2時間）

1. テスト実行（--limit 10）
2. BigQueryで結果確認
3. 全件処理（--limit 0）
4. ログをGitHubにコミット

### ステップ4: トラブルシューティング（所要時間: 1時間）

1. わざとエラーを発生させて対処法を学ぶ
2. 重複を作って削除してみる
3. BigQueryのクエリを試す

---

## 🔄 将来の拡張

### 予定されている改善（優先度順）

1. **TB判定の強化（Must-1）**
   - 政府短期証券の自動判定精度向上
   - 優先度: 高

2. **単位の拡張**
   - 「兆+万円」対応
   - 「百万円」対応
   - 優先度: 中

3. **GitHub Actionsでの自動化**
   - 定期実行（毎日0時）
   - 自動コミット
   - 優先度: 中

4. **エラー通知**
   - Slack連携
   - メール通知
   - 優先度: 低

### 拡張時の注意点

1. **単位を追加する場合**
   ```python
   # ALLOWED_UNITS に追加
   ALLOWED_UNITS = {'兆', '億', '円', '万', '百万'}
   
   # AMOUNT_PATTERNS に新しいパターンを追加
   (re.compile(r'([0-9,，]+)\s*万円'), 'man', '万円表記', 75),
   ```

2. **新しいパターンを追加する場合**
   - 優先度を既存パターンと調整
   - テストデータで検証
   - --limit 5 で動作確認

3. **スキーマを変更する場合**
   - --reset で再作成必須
   - バックアップを取る
   - テストデータセットで先に試す

---

## 📞 サポート連絡先

### 技術サポート

- **GitHub Issues**: https://github.com/planet9-cpri/jgb-database-project/issues
- **ドキュメント**: https://github.com/planet9-cpri/jgb-database-project/tree/main/docs

### エスカレーション

1. **レベル1**: ドキュメント確認
2. **レベル2**: ログ分析
3. **レベル3**: GitHub Issuesで質問
4. **レベル4**: 開発者に連絡

---

## ✅ 引き継ぎチェックリスト

### 前任者（引き継ぎ元）

- [ ] v7_fixed7の実行確認（テスト＋全件）
- [ ] GitHubへのコミット完了
- [ ] ドキュメントのアップデート
- [ ] 認証情報の引き継ぎ
- [ ] 実行デモの実施

### 後任者（引き継ぎ先）

- [ ] リポジトリのクローン
- [ ] Python環境のセットアップ
- [ ] 認証情報の設定確認
- [ ] テスト実行（--limit 5）
- [ ] 全ドキュメントの確認
- [ ] BigQueryアクセス確認
- [ ] 日次運用の理解
- [ ] トラブルシューティングの理解

---

## 🎉 最終確認事項

### 2025-10-31時点での状態

✅ **スクリプト**: batch_direct_processing_v7_fixed7.py（完成）  
✅ **テスト実行**: 20件、重複0件  
✅ **全件処理**: 462件、重複0件  
✅ **ログ**: GitHubにコミット済み  
✅ **ドキュメント**: 完備  
✅ **BigQueryテーブル**: 正常動作  

**全システムが本番運用可能な状態です！** 🚀

---

## 📝 更新履歴

| 日付 | バージョン | 内容 |
|------|-----------|------|
| 2025-10-31 | v7_fixed7 | 真・最終完全版リリース、引き継ぎドキュメント作成 |
| 2025-10-31 | v7_fixed6 | 本番運用最終版リリース |
| 2025-10-31 | v7_fixed5 | 本番運用対応版リリース |

---

**引き継ぎ完了を確認してください！** ✅

何か質問があれば、GitHub Issuesで質問してください。
