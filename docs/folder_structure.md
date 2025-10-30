# プロジェクトフォルダ構造 - 詳細説明

**最終更新**: 2025年10月23日  
**整理実施**: Day 5 Phase 1

---

## 📁 全体構造

```
C:\Users\sonke\projects\jgb_database_project\
│
├── 📄 README.md                    # プロジェクト概要
├── 📄 project_summary.md           # 詳細サマリー（引き継ぎ用）
├── 📄 requirements.txt             # Python依存関係
├── 📄 .env                         # 環境変数（Git除外）
├── 📄 .env.example                 # 環境変数サンプル
├── 📄 .gitignore                   # Git除外設定
├── 📄 project_config.json          # プロジェクト設定
│
├── 📁 config\                      # 設定ファイル
├── 📁 parsers\                     # パーサーモジュール
├── 📁 scripts\                     # 実行スクリプト
├── 📁 tests\                       # テストスクリプト
├── 📁 docs\                        # ドキュメント
├── 📁 data\                        # データファイル
├── 📁 sql\                         # SQLクエリ
├── 📁 logs\                        # ログファイル
├── 📁 output\                      # 出力ファイル
├── 📁 archive\                     # アーカイブ
├── 📁 api\                         # API（将来用）
└── 📁 database\                    # データベース関連（将来用）
```

---

## 📂 各フォルダの詳細

### 📁 config/
**目的**: プロジェクト全体の設定ファイル

```
config/
├── __init__.py
└── settings.py              # 設定管理
```

**使用例**:
```python
from config.settings import PROJECT_ID, DATASET_ID
```

---

### 📁 parsers/
**目的**: 官報・告示の解析モジュール

```
parsers/
├── __init__.py
├── table_parser.py          # 国債情報抽出（メイン）
├── issue_extractor.py       # 発行情報抽出
├── kanpo_parser.py          # 官報パーサー
└── vertical_table_parser.py # 縦書き表対応（オプション）
```

**主要モジュール**:
- **table_parser.py**
  - 別表形式の解析
  - 単一銘柄告示の解析
  - `BondIssuance` データクラス定義

- **issue_extractor.py**
  - 複数銘柄の一括抽出
  - `IssueExtractor` クラス

**使用例**:
```python
from parsers.table_parser import TableParser
parser = TableParser()
result = parser.extract_bond_info_from_single(content)
```

---

### 📁 scripts/
**目的**: 実行可能なスクリプト（機能別に分類）

#### 📁 scripts/01_data_ingestion/
データ投入・読み込みスクリプト

```
01_data_ingestion/
├── __init__.py
├── batch_issue_extractor.py    # 官報から一括抽出
├── upload_issues_to_bigquery.py # BigQuery投入
├── load_issuance_data.py       # 発行データ読み込み
└── load_masters.py             # マスタデータ読み込み
```

**主要スクリプト**:
- `batch_issue_extractor.py`: 複数ファイルから発行情報を抽出
- `upload_issues_to_bigquery.py`: BigQueryへデータ投入

#### 📁 scripts/02_data_correction/
データ修正スクリプト

```
02_data_correction/
├── __init__.py
└── fix_bond_master_ids.py     # BOND_001修正（Day 4）
```

**主要スクリプト**:
- `fix_bond_master_ids.py`: 
  - BOND_001を正しいbond_master_idに修正
  - 特殊債券の自動追加機能
  - Day 4で作成

#### 📁 scripts/03_data_validation/
データ検証スクリプト

```
03_data_validation/
├── __init__.py
└── check_table_schemas.py     # スキーマ確認
```

**今後追加予定**:
- `validate_amounts.py`: 発行額の検証
- `verify_totals.py`: 合計値の検証

#### 📁 scripts/04_utilities/
ユーティリティスクリプト

```
04_utilities/
├── __init__.py
├── check_bq_quota.py           # BigQueryクォータ確認
├── test_bigquery_connection.py # 接続テスト
├── debug_data_inspection.py    # データ調査
├── check_file_content.py       # ファイル内容確認
├── execute_table_creation.py   # テーブル作成
├── recreate_tables.py          # テーブル再作成
├── test_kanji_conversion.py    # 漢字変換テスト
└── test_kanpo_parser.py        # 官報パーサーテスト
```

---

### 📁 tests/
**目的**: テストスクリプトとテストデータ

```
tests/
├── __init__.py
├── fixtures/                    # テストデータ
│   ├── 利付債券（別表4列）.txt
│   ├── 利付債券（別表5列）.txt
│   ├── 利付債券（別表なし）.txt
│   ├── 利付債券（個人向け）.txt
│   ├── 利付債券（物価連動）.txt
│   ├── 利付債券（GX債権）.txt
│   ├── 利付債券（入札発行）.txt
│   └── 国庫短期証券（復興債）.txt
├── test_table_parser.py         # TableParserのテスト
├── test_tanki_extraction.py     # 国庫短期証券テスト（Day 4）
├── test_single_update.py        # 単一ファイルテスト（Day 4）
├── test_notice_parser_integration.py # 統合テスト
├── debug_table_parser.py        # デバッグ用
├── debug_table_parser_v2.py     # デバッグ用v2
└── debug_table_parser_v3.py     # デバッグ用v3
```

**Day 4で追加**:
- `test_tanki_extraction.py`: 国庫短期証券の抽出テスト
- `test_single_update.py`: 1件の抽出・マッピング・DB確認テスト

---

### 📁 docs/
**目的**: プロジェクトドキュメント

```
docs/
├── project_summary.md           # プロジェクトサマリー（Day 5）
├── progress_log.md              # 進捗ログ（Day 5）
├── handoff_template.md          # 引き継ぎテンプレート（Day 5）
├── folder_structure.md          # このファイル（Day 5）
├── day3_handoff_template.md     # Day 3の引き継ぎ（旧）
├── new_thread_template.md       # 新スレッドテンプレート（旧）
├── api/                         # API仕様（将来用）
├── design/                      # 設計ドキュメント
└── operation/                   # 運用ドキュメント
```

**Day 5で作成**:
- `project_summary.md`: 新チャット引き継ぎの中核ドキュメント
- `progress_log.md`: Day 1～5の詳細な作業履歴
- `handoff_template.md`: 新チャット開始時のメッセージテンプレート
- `folder_structure.md`: フォルダ構造の詳細説明

---

### 📁 data/
**目的**: プロジェクトで使用するデータファイル

```
data/
├── kanpo_2023/                  # 官報データ（2023年度）
├── masters/                     # マスタデータ
│   ├── bonds_master.csv        # 国債マスタ
│   ├── laws_master.csv         # 法令マスタ
│   └── law_articles_master.csv # 法令条項マスタ
└── processed/                   # 処理済みデータ
```

**注意**: 
- `G:\マイドライブ\JGBデータ\2023\` が実際の告示ファイルの場所
- この`data/`フォルダは補助的なデータ保存用

---

### 📁 sql/
**目的**: SQLクエリとスクリプト

```
sql/
├── create_tables.sql            # テーブル作成DDL
└── queries/                     # クエリ集
    ├── validation/              # 検証用（将来追加）
    └── analysis/                # 分析用（将来追加）
```

**将来追加予定**:
- `queries/validation/check_bond_001.sql`
- `queries/validation/verify_totals.sql`
- `queries/analysis/bond_summary.sql`

---

### 📁 logs/
**目的**: 実行ログとレポート

```
logs/
├── organize_report_20251023_205234.txt  # 整理レポート（Day 5）
└── (その他の実行ログ)
```

**ログの種類**:
- スクリプト実行ログ
- エラーログ
- 整理・移動レポート

---

### 📁 output/
**目的**: スクリプトの出力ファイル

```
output/
├── logs/                        # 実行ログ
├── reports/                     # レポート
└── temp/                        # 一時ファイル
    └── extraction_results.json  # 抽出結果（Day 5で移動）
```

---

### 📁 archive/
**目的**: 古いファイル・バックアップの保管

```
archive/
├── old_scripts/                 # 旧スクリプト
└── parsers/                     # パーサーのバックアップ
    ├── table_parser.py.backup   # バックアップファイル（Day 5で移動）
    └── old/                     # 旧バージョン（Day 5で移動）
        └── table_parser.py.old
```

---

### 📁 api/ ✨将来用
**目的**: REST API実装（Phase拡張時）

```
api/
├── __init__.py
├── models/                      # データモデル
└── routers/                     # APIルーティング
```

---

### 📁 database/ ✨将来用
**目的**: データベース関連モジュール（Phase拡張時）

```
database/
└── __init__.py
```

---

## 🔧 重要なファイル

### ルートディレクトリ

#### README.md
プロジェクトの概要とクイックスタート

#### project_summary.md ⭐
**最重要**: 新チャット引き継ぎの中核ドキュメント
- プロジェクト全体のサマリー
- 技術スタック
- データ状況
- 次のステップ

#### requirements.txt
Python依存パッケージ
```
google-cloud-bigquery
google-auth
...
```

#### .env
環境変数（Git除外）
```
GOOGLE_APPLICATION_CREDENTIALS=C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json
PROJECT_ID=jgb2023
DATASET_ID=20251019
```

---

## 📝 命名規則

### フォルダ
- **小文字**: `config`, `parsers`, `scripts`
- **複数形**: `tests`, `docs`, `logs`
- **番号プレフィックス**: `01_data_ingestion`, `02_data_correction`

### ファイル
- **スクリプト**: `動詞_対象.py`
  - 例: `load_masters.py`, `fix_bond_master_ids.py`
- **テスト**: `test_対象.py`
  - 例: `test_table_parser.py`
- **ドキュメント**: `名詞_名詞.md`
  - 例: `project_summary.md`, `progress_log.md`

### モジュール/クラス
- **クラス**: PascalCase
  - 例: `TableParser`, `IssueExtractor`
- **関数**: snake_case
  - 例: `extract_bond_info_from_single()`

---

## 🚀 使い方

### 新しいスクリプトを追加する場合

1. **目的に応じてフォルダを選択**
   - データ投入 → `scripts/01_data_ingestion/`
   - データ修正 → `scripts/02_data_correction/`
   - データ検証 → `scripts/03_data_validation/`
   - その他 → `scripts/04_utilities/`

2. **適切な命名**
   - `動詞_対象.py` の形式

3. **__init__.pyの確認**
   - サブフォルダに`__init__.py`があることを確認

### ドキュメントを追加する場合

1. **docs/フォルダに配置**
2. **Markdown形式で記述**
3. **project_summary.mdにリンク追加**

---

## 📊 フォルダ統計

### ファイル数（Day 5 Phase 1完了時）
- Pythonファイル: 3787件
- Markdownファイル: 8件
- SQLファイル: 1件

### 主要フォルダのファイル数
- `scripts/`: 14ファイル（4つのサブフォルダに分類）
- `parsers/`: 4ファイル
- `tests/`: 15ファイル
- `docs/`: 8ファイル

---

**このドキュメントはフォルダ構造が変更された際に更新すること**