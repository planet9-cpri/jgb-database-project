"""
BigQueryのクォータと設定を確認するスクリプト

使用方法:
    python scripts/check_bq_quota.py
"""

import os
from google.cloud import bigquery
from google.api_core import exceptions

# 設定
PROJECT_ID = "jgb2023"
DATASET_ID = "20251019"
SERVICE_ACCOUNT_KEY = r"C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json"

def check_bigquery_config():
    """BigQueryの設定を確認"""
    print("="*60)
    print("🔍 BigQuery設定チェック")
    print("="*60)
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY
    client = bigquery.Client(project=PROJECT_ID)
    
    print("\n【1. 基本情報】")
    print(f"  プロジェクトID: {client.project}")
    print(f"  ロケーション: {client.location or 'デフォルト'}")
    
    # プロジェクト情報
    try:
        project = client.get_project(PROJECT_ID)
        print(f"  プロジェクト名: {project.friendly_name or 'N/A'}")
    except Exception as e:
        print(f"  ⚠️ プロジェクト情報取得エラー: {e}")
    
    print("\n【2. データセット情報】")
    try:
        dataset = client.get_dataset(f"{PROJECT_ID}.{DATASET_ID}")
        print(f"  データセットID: {dataset.dataset_id}")
        print(f"  ロケーション: {dataset.location}")
        print(f"  作成日時: {dataset.created}")
        print(f"  最終更新: {dataset.modified}")
        
        # データセットのアクセス設定
        print(f"\n  【アクセス権限】")
        for entry in dataset.access_entries:
            print(f"    - {entry.role}: {entry.entity_type} ({entry.entity_id})")
        
    except exceptions.NotFound:
        print(f"  ❌ データセットが見つかりません: {DATASET_ID}")
        return
    except Exception as e:
        print(f"  ⚠️ データセット情報取得エラー: {e}")
    
    print("\n【3. テーブル情報】")
    try:
        tables = list(client.list_tables(dataset))
        print(f"  テーブル数: {len(tables)}")
        
        for table in tables:
            table_ref = client.get_table(table.reference)
            print(f"\n  📊 {table.table_id}")
            print(f"    - 行数: {table_ref.num_rows:,}")
            print(f"    - サイズ: {table_ref.num_bytes / 1024 / 1024:.2f} MB")
            print(f"    - パーティション: {table_ref.time_partitioning.type if table_ref.time_partitioning else 'なし'}")
            
    except Exception as e:
        print(f"  ⚠️ テーブル情報取得エラー: {e}")
    
    print("\n【4. APIテスト: 小規模クエリ実行】")
    try:
        query = f"""
        SELECT COUNT(*) as count
        FROM `{PROJECT_ID}.{DATASET_ID}.laws_master`
        """
        result = client.query(query).result()
        row = list(result)[0]
        print(f"  ✅ クエリ実行成功: laws_master = {row['count']} 行")
        
    except exceptions.Forbidden as e:
        print(f"  ❌ 権限エラー: {e}")
    except exceptions.NotFound as e:
        print(f"  ❌ テーブル未発見: {e}")
    except Exception as e:
        print(f"  ⚠️ クエリ実行エラー: {e}")
    
    print("\n【5. データ投入テスト: 極小データ】")
    try:
        import pandas as pd
        from datetime import datetime
        
        # 極小のテストデータ
        test_data = pd.DataFrame([{
            'announcement_id': 'TEST_001',
            'kanpo_date': datetime(2023, 1, 1),
            'announcement_number': 'テスト',
            'gazette_issue_number': '',
            'announcement_type': 'テスト',
            'title': '',
            'source_file': 'test.txt',
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }])
        
        table_id = f"{PROJECT_ID}.{DATASET_ID}.announcements"
        
        print(f"  📊 テストデータを投入中...")
        print(f"  対象テーブル: {table_id}")
        print(f"  データサイズ: {len(test_data)} 行")
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        
        job = client.load_table_from_dataframe(
            test_data, table_id, job_config=job_config
        )
        
        print(f"  ⏳ ジョブID: {job.job_id}")
        print(f"  ⏳ ジョブ状態: {job.state}")
        
        # 結果を待機（タイムアウト30秒）
        job.result(timeout=30)
        
        print(f"  ✅ データ投入成功！")
        print(f"  📊 最終状態: {job.state}")
        
        # テストデータを削除
        print(f"\n  🧹 テストデータを削除中...")
        delete_query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET_ID}.announcements`
        WHERE announcement_id = 'TEST_001'
        """
        client.query(delete_query).result()
        print(f"  ✅ テストデータ削除完了")
        
    except exceptions.ServiceUnavailable as e:
        print(f"  ❌ 503 Service Unavailable: {e}")
        print(f"  💡 これは一時的なサービス障害です")
    except exceptions.Forbidden as e:
        print(f"  ❌ 権限エラー: {e}")
        print(f"  💡 サービスアカウントの権限を確認してください")
    except Exception as e:
        print(f"  ⚠️ データ投入テスト失敗: {e}")
        print(f"  💡 エラータイプ: {type(e).__name__}")
    
    print("\n【6. サービスアカウント情報】")
    try:
        print(f"  認証ファイル: {SERVICE_ACCOUNT_KEY}")
        print(f"  プロジェクト: {client.project}")
        
        # サービスアカウントのメールアドレスを取得
        from google.oauth2 import service_account
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_KEY
        )
        print(f"  サービスアカウント: {credentials.service_account_email}")
        print(f"  プロジェクトID: {credentials.project_id}")
        
    except Exception as e:
        print(f"  ⚠️ サービスアカウント情報取得エラー: {e}")
    
    print("\n" + "="*60)
    print("✅ 設定チェック完了")
    print("="*60)
    
    print("\n【7. 推奨される確認事項】")
    print("  1. Google Cloud Console でクォータ使用状況を確認")
    print("     https://console.cloud.google.com/iam-admin/quotas")
    print("  2. 課金が有効になっているか確認")
    print("     https://console.cloud.google.com/billing")
    print("  3. BigQuery API が有効になっているか確認")
    print("     https://console.cloud.google.com/apis/library/bigquery.googleapis.com")


if __name__ == "__main__":
    check_bigquery_config()