"""
BigQuery接続テスト
"""
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from google.cloud import bigquery
from google.oauth2 import service_account
from config.settings import BIGQUERY_CONFIG

def test_connection():
    print("=" * 60)
    print("BigQuery接続テスト")
    print("=" * 60)
    
    try:
        # 認証情報の読み込み
        print(f"\n1. 認証情報を読み込み中...")
        print(f"   パス: {BIGQUERY_CONFIG['credentials_path']}")
        
        credentials = service_account.Credentials.from_service_account_file(
            BIGQUERY_CONFIG['credentials_path']
        )
        
        # クライアント作成
        print(f"\n2. BigQueryクライアントを作成中...")
        client = bigquery.Client(
            credentials=credentials,
            project=BIGQUERY_CONFIG['project_id']
        )
        
        # データセット確認
        print(f"\n3. データセットを確認中...")
        dataset_id = f"{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}"
        dataset = client.get_dataset(dataset_id)
        
        print(f"\n✅ 接続成功！")
        print(f"   プロジェクト: {BIGQUERY_CONFIG['project_id']}")
        print(f"   データセット: {BIGQUERY_CONFIG['dataset_id']}")
        print(f"   ロケーション: {dataset.location}")
        
        # テーブル一覧
        print(f"\n4. テーブル一覧:")
        tables = list(client.list_tables(dataset_id))
        if tables:
            for table in tables:
                print(f"   - {table.table_id}")
        else:
            print("   （テーブルなし）")
        
        print("\n" + "=" * 60)
        return True
        
    except Exception as e:
        print(f"\n❌ 接続エラー:")
        print(f"   {e}")
        print("\n" + "=" * 60)
        return False

if __name__ == '__main__':
    success = test_connection()
    sys.exit(0 if success else 1)
