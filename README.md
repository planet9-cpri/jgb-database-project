# JGB Database Project

国債データベース管理システム

## Phase 1: 国債発行データベース化（2023年度）

### 環境情報
- Python: 3.10+
- BigQuery: GCP
- OS: Windows 11

### クイックスタート
```bash
# 仮想環境作成
python -m venv venv
venv\Scripts\activate

# パッケージインストール
pip install -r requirements.txt

# BigQuery接続テスト
python scripts/test_bigquery_connection.py
```

### プロジェクト構成
```
jgb_database_project/
├── config/          # 設定ファイル
├── database/        # BigQueryクライアント
├── parsers/         # 官報パーサー
├── api/             # REST API
├── data/            # データファイル
├── sql/             # SQLスクリプト
├── tests/           # テストコード
├── scripts/         # ユーティリティ
└── docs/            # ドキュメント
```

### チーム
- Person A: マスタ管理
- Person B: パーサー実装
- Person C: インフラ・API

### 進捗
- [x] プロジェクトセットアップ
- [ ] Week 1: 設計・環境構築
- [ ] Week 2-3: マスタ準備・パーサー実装
- [ ] Week 4: データ投入
- [ ] Week 5: 検証
- [ ] Week 6: ドキュメント・完了
