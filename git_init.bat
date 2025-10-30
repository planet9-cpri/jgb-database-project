@echo off
REM JGB Database Project - Git初期化スクリプト
REM 作成日: 2025-10-30

echo ================================================================================
echo JGB Database Project - Git リポジトリ初期化
echo ================================================================================
echo.

REM プロジェクトディレクトリに移動
cd /d "C:\Users\sonke\projects\jgb_database_project"

REM Gitが既に初期化されているか確認
if exist ".git" (
    echo [情報] Git リポジトリは既に初期化されています
    echo.
    goto :status
)

REM Git初期化
echo [1/4] Git リポジトリを初期化しています...
git init
echo   ✓ 完了
echo.

REM .gitignoreファイルの作成
echo [2/4] .gitignore ファイルを作成しています...
(
echo # Python
echo __pycache__/
echo *.py[cod]
echo *$py.class
echo *.so
echo .Python
echo env/
echo venv/
echo .venv/
echo.
echo # 認証情報 ^(絶対にコミットしない^)
echo *.json
echo secrets/
echo *.env
echo .env
echo.
echo # データファイル ^(大容量^)
echo data/
echo *.csv
echo *.xlsx
echo.
echo # 官報データは除外 ^(大容量^)
echo # ※パスは適宜調整
echo.
echo # ログ
echo logs/
echo *.log
echo.
echo # IDE
echo .vscode/
echo .idea/
echo *.swp
echo *.swo
echo.
echo # OS
echo .DS_Store
echo Thumbs.db
echo.
echo # BigQuery一時ファイル
echo .bigquery/
echo.
echo # 一時ファイル
echo temp/
echo tmp/
echo *.tmp
) > .gitignore
echo   ✓ 完了
echo.

REM READMEファイルの作成
echo [3/4] README.md を作成しています...
(
echo # JGB Database Project
echo.
echo ## プロジェクト概要
echo 2023年度の日本国債発行データを官報から抽出し、BigQueryデータベースに格納するプロジェクト。
echo.
echo ## 環境
echo - Python 3.10+
echo - BigQuery ^(GCP^)
echo - プロジェクトID: jgb2023
echo - データセットID: 20251029
echo.
echo ## ディレクトリ構造
echo ```
echo jgb_database_project/
echo ├── parsers/              # パーサーモジュール
echo ├── scripts/
echo │   ├── 01_data_ingestion/
echo │   ├── 02_data_correction/
echo │   ├── 03_data_validation/
echo │   └── 04_utilities/
echo ├── tests/
echo ├── docs/
echo ├── data/
echo ├── logs/
echo └── output/
echo ```
echo.
echo ## セットアップ
echo ```bash
echo # 仮想環境の作成
echo python -m venv venv
echo venv\Scripts\activate
echo.
echo # 依存パッケージのインストール
echo pip install -r requirements.txt
echo ```
echo.
echo ## 現在のステータス
echo Phase 5: v9パーサー開発・テスト中
echo.
echo ## 最終更新
echo 2025-10-30
) > README.md
echo   ✓ 完了
echo.

REM 初回コミット
echo [4/4] 初回コミットを作成しています...
git add .
git commit -m "Initial commit: JGB Database Project Phase 5 - v9 parser development"
echo   ✓ 完了
echo.

:status
REM 現在の状態を表示
echo ================================================================================
echo Git リポジトリの状態
echo ================================================================================
git status
echo.
echo ================================================================================
echo コミット履歴
echo ================================================================================
git log --oneline --graph --decorate --all
echo.
echo ================================================================================
echo 初期化完了！
echo ================================================================================
echo.
echo 次のステップ:
echo   1. GitHub でリポジトリを作成（オプション）
echo   2. Google Drive にドキュメントフォルダを作成
echo   3. 開発を継続
echo.
pause