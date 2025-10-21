"""
BigQueryテーブル作成実行スクリプト
Phase 1用の6テーブルを作成

Author: Person C (Infrastructure & API)
"""

import sys
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

# ==================== 設定 ====================
PROJECT_ID = "jgb2023"
DATASET_ID = "20251019"
LOCATION = "asia-northeast1"
CREDENTIALS_PATH = Path(r"C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json")
SQL_FILE = Path(r"sql\create_tables.sql")

# ==================== テーブル定義 ====================
TABLES = [
    "laws_master",
    "law_articles_master",
    "bonds_master",
    "announcements",
    "bond_issuances",
    "issuance_legal_basis",
]


class TableCreator:
    """BigQueryテーブル作成管理クラス"""
    
    def __init__(self, project_id: str, dataset_id: str, credentials_path: Path):
        """
        初期化
        
        Args:
            project_id: GCPプロジェクトID
            dataset_id: BigQueryデータセットID
            credentials_path: 認証情報JSONファイルのパス
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.credentials_path = credentials_path
        self.client = None
        
        # 結果記録
        self.results = []
    
    def connect(self) -> bool:
        """
        BigQueryに接続
        
        Returns:
            接続成功ならTrue
        """
        try:
            print("🔌 BigQueryに接続中...")
            
            if not self.credentials_path.exists():
                print(f"❌ 認証情報が見つかりません: {self.credentials_path}")
                return False
            
            credentials = service_account.Credentials.from_service_account_file(
                str(self.credentials_path),
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            
            self.client = bigquery.Client(
                credentials=credentials,
                project=self.project_id,
                location=LOCATION
            )
            
            print(f"✅ 接続成功: {self.project_id}")
            return True
            
        except Exception as e:
            print(f"❌ 接続エラー: {e}")
            return False
    
    def check_dataset(self) -> bool:
        """
        データセットの存在確認
        
        Returns:
            データセットが存在すればTrue
        """
        try:
            dataset_id = f"{self.project_id}.{self.dataset_id}"
            dataset = self.client.get_dataset(dataset_id)
            
            print(f"✅ データセット確認: {dataset_id}")
            print(f"   作成日時: {dataset.created}")
            print(f"   ロケーション: {dataset.location}")
            
            return True
            
        except Exception as e:
            print(f"❌ データセットが見つかりません: {e}")
            return False
    
    def read_sql_file(self, sql_file: Path) -> str:
        """
        SQLファイルを読み込み
        
        Args:
            sql_file: SQLファイルのパス
            
        Returns:
            SQL文字列
        """
        if not sql_file.exists():
            raise FileNotFoundError(f"SQLファイルが見つかりません: {sql_file}")
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            return f.read()
    
    def execute_sql(self, sql: str, description: str = "") -> bool:
        """
        SQL文を実行
        
        Args:
            sql: 実行するSQL
            description: 処理の説明
            
        Returns:
            成功ならTrue
        """
        try:
            if description:
                print(f"\n📋 {description}")
            
            query_job = self.client.query(sql)
            query_job.result()  # 完了を待つ
            
            print(f"✅ 実行成功")
            return True
            
        except Exception as e:
            print(f"❌ 実行エラー: {e}")
            return False
    
    def create_tables_from_file(self, sql_file: Path) -> dict:
        """
        SQLファイルからテーブルを作成
        
        Args:
            sql_file: CREATE TABLE文を含むSQLファイル
            
        Returns:
            実行結果のサマリー
        """
        print("\n" + "=" * 60)
        print("📦 テーブル作成開始")
        print("=" * 60)
        
        try:
            # SQLファイル読み込み
            sql_content = self.read_sql_file(sql_file)
            print(f"✅ SQLファイル読み込み完了: {sql_file}")
            
            # CREATE TABLE文ごとに分割して実行
            statements = self._split_sql_statements(sql_content)
            
            success_count = 0
            for i, stmt in enumerate(statements, 1):
                if stmt.strip().upper().startswith('CREATE TABLE'):
                    # テーブル名を抽出
                    table_name = self._extract_table_name(stmt)
                    
                    print(f"\n[{i}/{len(statements)}] テーブル作成中: {table_name}")
                    
                    if self.execute_sql(stmt):
                        success_count += 1
                        self.results.append({
                            'table': table_name,
                            'status': 'success',
                            'timestamp': datetime.now()
                        })
                    else:
                        self.results.append({
                            'table': table_name,
                            'status': 'failed',
                            'timestamp': datetime.now()
                        })
            
            return {
                'total': len([s for s in statements if 'CREATE TABLE' in s.upper()]),
                'success': success_count,
                'failed': len(statements) - success_count
            }
            
        except Exception as e:
            print(f"❌ エラー: {e}")
            return {'total': 0, 'success': 0, 'failed': 0}
    
    def _split_sql_statements(self, sql: str) -> list:
        """
        SQL文を個別のステートメントに分割
        
        Args:
            sql: 複数のSQL文を含む文字列
            
        Returns:
            個別のSQL文のリスト
        """
        # コメント除去
        lines = []
        for line in sql.split('\n'):
            # 行コメント除去
            if '--' in line:
                line = line[:line.index('--')]
            lines.append(line)
        
        sql = '\n'.join(lines)
        
        # セミコロンで分割
        statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]
        return statements
    
    def _extract_table_name(self, create_statement: str) -> str:
        """
        CREATE TABLE文からテーブル名を抽出
        
        Args:
            create_statement: CREATE TABLE文
            
        Returns:
            テーブル名
        """
        import re
        match = re.search(r'CREATE TABLE.*?`([^`]+)`', create_statement, re.IGNORECASE)
        if match:
            full_name = match.group(1)
            # project.dataset.table から table部分のみ抽出
            return full_name.split('.')[-1]
        return "unknown"
    
    def verify_tables(self) -> dict:
        """
        作成されたテーブルを検証
        
        Returns:
            検証結果の辞書
        """
        print("\n" + "=" * 60)
        print("🔍 テーブル検証")
        print("=" * 60)
        
        dataset_id = f"{self.project_id}.{self.dataset_id}"
        tables = list(self.client.list_tables(dataset_id))
        
        existing_tables = [table.table_id for table in tables]
        
        verification = {
            'expected': TABLES,
            'found': existing_tables,
            'missing': [t for t in TABLES if t not in existing_tables],
            'extra': [t for t in existing_tables if t not in TABLES]
        }
        
        print(f"\n📊 検証結果:")
        print(f"  期待テーブル数: {len(verification['expected'])}")
        print(f"  発見テーブル数: {len(verification['found'])}")
        
        if verification['missing']:
            print(f"\n⚠️  未作成のテーブル:")
            for table in verification['missing']:
                print(f"    - {table}")
        
        if verification['extra']:
            print(f"\n📌 追加テーブル:")
            for table in verification['extra']:
                print(f"    - {table}")
        
        # テーブル詳細情報
        print(f"\n📋 テーブル詳細:")
        for table in tables:
            full_table = self.client.get_table(f"{dataset_id}.{table.table_id}")
            print(f"\n  {table.table_id}:")
            print(f"    カラム数: {len(full_table.schema)}")
            print(f"    作成日時: {full_table.created}")
            print(f"    行数: {full_table.num_rows}")
        
        return verification
    
    def print_summary(self):
        """実行結果サマリーを表示"""
        print("\n" + "=" * 60)
        print("📊 実行結果サマリー")
        print("=" * 60)
        
        success = [r for r in self.results if r['status'] == 'success']
        failed = [r for r in self.results if r['status'] == 'failed']
        
        print(f"\n✅ 成功: {len(success)} テーブル")
        for result in success:
            print(f"  - {result['table']}")
        
        if failed:
            print(f"\n❌ 失敗: {len(failed)} テーブル")
            for result in failed:
                print(f"  - {result['table']}")


def main():
    """メイン処理"""
    print("=" * 60)
    print("🚀 JGB Database - テーブル作成スクリプト")
    print("=" * 60)
    print(f"\nプロジェクト: {PROJECT_ID}")
    print(f"データセット: {DATASET_ID}")
    print(f"認証情報: {CREDENTIALS_PATH}")
    print(f"SQLファイル: {SQL_FILE}")
    
    # TableCreatorインスタンス化
    creator = TableCreator(PROJECT_ID, DATASET_ID, CREDENTIALS_PATH)
    
    # 接続
    if not creator.connect():
        print("\n❌ 接続に失敗しました")
        sys.exit(1)
    
    # データセット確認
    if not creator.check_dataset():
        print("\n❌ データセットが見つかりません")
        sys.exit(1)
    
    # テーブル作成
    if SQL_FILE.exists():
        result = creator.create_tables_from_file(SQL_FILE)
        
        print("\n" + "=" * 60)
        print(f"✅ テーブル作成完了")
        print(f"  成功: {result['success']} / {result['total']}")
        print("=" * 60)
    else:
        print(f"\n⚠️  SQLファイルが見つかりません: {SQL_FILE}")
        print("   手動でテーブルを作成しますか？ (y/n): ", end='')
        response = input().strip().lower()
        
        if response == 'y':
            print("\n手動作成モードは未実装です")
            sys.exit(1)
    
    # テーブル検証
    verification = creator.verify_tables()
    
    # サマリー表示
    creator.print_summary()
    
    # 完了判定
    if len(verification['missing']) == 0:
        print("\n🎉 すべてのテーブルが正常に作成されました！")
        print("\n📋 次のステップ:")
        print("  1. マスタデータの投入（Person A）")
        print("  2. パーサーの実装継続（Person B）")
        print("  3. API基盤の構築開始（Person C）")
        sys.exit(0)
    else:
        print("\n⚠️  一部のテーブルが作成されていません")
        print("   エラーを確認して再実行してください")
        sys.exit(1)


if __name__ == "__main__":
    main()