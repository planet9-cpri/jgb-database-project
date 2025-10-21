"""
マスタデータをBigQueryに投入するスクリプト

使用方法:
    python scripts/load_masters.py

機能:
    - data/masters/ 配下のCSVファイルをBigQueryに投入
    - データ型を自動変換（BOOLEAN, DATE, INTEGER, NUMERIC）
    - タイムスタンプ（created_at, updated_at）を自動追加
    - 投入後にデータ検証を実行
"""

from google.cloud import bigquery
from pathlib import Path
import pandas as pd
from datetime import datetime
import numpy as np

# ==================== 設定 ====================

credentials_path = r"C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json"
project_id = "jgb2023"
dataset_id = "20251019"
masters_dir = Path("data/masters")

# ==================== 関数定義 ====================

def convert_data_types(df, table_name):
    """
    DataFrameのデータ型をBigQueryのスキーマに合わせて変換
    
    Args:
        df: pandas DataFrame
        table_name: テーブル名
        
    Returns:
        変換後のDataFrame
    """
    print(f"  🔄 データ型を変換中...")
    
    # BOOLEAN型の変換
    if 'is_active' in df.columns:
        df['is_active'] = df['is_active'].map({
            'TRUE': True, 'True': True, 'true': True,
            'FALSE': False, 'False': False, 'false': False,
            True: True, False: False
        })
        print(f"    ✓ is_active → BOOLEAN")
    
    # DATE型の変換
    date_columns = ['promulgation_date', 'enforcement_date', 'effective_from', 'effective_to']
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].replace('', pd.NA)
            df[col] = pd.to_datetime(df[col], errors='coerce')
            print(f"    ✓ {col} → DATE")
    
    # INTEGER型の変換（完全修正版）
    if 'maturity_years' in df.columns:
        try:
            # 空文字列とNoneをNaNに変換
            df['maturity_years'] = df['maturity_years'].replace(['', None, 'nan', 'NaN'], pd.NA)
            
            # まず数値型に変換
            df['maturity_years'] = pd.to_numeric(df['maturity_years'], errors='coerce')
            
            # NaNでない値を整数に丸める（小数点以下を削除）
            # これでfloat64 -> int64の変換が安全になる
            df['maturity_years'] = df['maturity_years'].apply(
                lambda x: pd.NA if pd.isna(x) else int(round(x))
            )
            
            # Int64型に変換
            df['maturity_years'] = df['maturity_years'].astype('Int64')
            
            print(f"    ✓ maturity_years → INTEGER")
        except Exception as e:
            print(f"    ⚠️  maturity_years変換エラー: {e}")
            # エラー時はそのまま継続
    
    # NUMERIC型の変換
    if 'min_denomination' in df.columns:
        df['min_denomination'] = df['min_denomination'].replace(['', None], pd.NA)
        df['min_denomination'] = pd.to_numeric(df['min_denomination'], errors='coerce')
        print(f"    ✓ min_denomination → NUMERIC")
    
    # タイムスタンプカラムを追加
    current_time = pd.Timestamp.now(tz='UTC')
    if 'created_at' not in df.columns:
        df['created_at'] = current_time
        print(f"    ✓ created_at → TIMESTAMP (自動追加)")
    if 'updated_at' not in df.columns:
        df['updated_at'] = current_time
        print(f"    ✓ updated_at → TIMESTAMP (自動追加)")
    
    # 空文字列をNoneに変換
    df = df.replace('', None)
    
    return df


def load_master_table(client, table_name, csv_file):
    """
    単一のマスタテーブルをBigQueryに投入
    
    Args:
        client: BigQueryクライアント
        table_name: テーブル名
        csv_file: CSVファイルのパス
        
    Returns:
        (成功/失敗, エラーメッセージ)
    """
    print(f"\n📦 {table_name} を投入中...")
    print(f"  📄 ファイル: {csv_file}")
    
    try:
        # CSVを読み込み
        df = pd.read_csv(csv_file)
        print(f"  ✅ CSV読み込み成功: {len(df)}行")
        print(f"  📊 カラム: {list(df.columns)}")
        
        # データ型を変換
        df = convert_data_types(df, table_name)
        
        # BigQueryテーブル参照
        table_ref = f"{project_id}.{dataset_id}.{table_name}"
        
        # データを投入
        print(f"  🚀 BigQueryに投入中...")
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # 既存データを上書き
            autodetect=False,  # 自動検出を無効化（スキーマは既に定義済み）
        )
        
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # 完了を待つ
        
        print(f"  ✅ 投入成功: {len(df)}行")
        return True, None
        
    except Exception as e:
        print(f"  ❌ 投入エラー: {e}")
        return False, str(e)


def verify_data(client, table_name):
    """
    投入したデータを検証
    
    Args:
        client: BigQueryクライアント
        table_name: テーブル名
    """
    table_ref = f"{project_id}.{dataset_id}.{table_name}"
    
    try:
        # 行数をカウント
        query = f"""
        SELECT COUNT(*) as row_count
        FROM `{table_ref}`
        """
        
        result = client.query(query).to_dataframe()
        row_count = result['row_count'].iloc[0]
        
        print(f"📊 {table_name}:")
        print(f"  行数: {row_count}")
        
        # サンプルデータを表示
        if row_count > 0:
            sample_query = f"""
            SELECT *
            FROM `{table_ref}`
            LIMIT 3
            """
            sample_df = client.query(sample_query).to_dataframe()
            print(f"  カラム数: {len(sample_df.columns)}")
            print(f"  サンプルデータ(最初の3行):")
            print(sample_df.to_string(index=False))
        else:
            # スキーマ情報だけ表示
            table = client.get_table(table_ref)
            print(f"  カラム数: {len(table.schema)}")
            print(f"  サンプルデータ: なし")
    
    except Exception as e:
        print(f"  ⚠️  検証エラー: {e}")


# ==================== メイン処理 ====================

def main():
    """メイン処理"""
    
    # ヘッダー表示
    print("\n" + "="*60)
    print("🚀 マスタデータBigQuery投入スクリプト")
    print("="*60)
    print(f"プロジェクト: {project_id}")
    print(f"データセット: {dataset_id}")
    print(f"認証情報: {credentials_path}")
    print(f"マスタディレクトリ: {masters_dir}")
    
    # BigQueryクライアント作成
    print("\n🔌 BigQueryに接続中...")
    client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)
    print(f"✅ 接続成功: {client.project}")
    
    # マスタテーブルの定義（投入順序も考慮）
    master_tables = [
        "laws_master",
        "law_articles_master", 
        "bonds_master"
    ]
    
    # 投入処理
    print("\n" + "="*60)
    print("📦 マスタデータ投入開始")
    print("="*60)
    
    success_count = 0
    failed_tables = []
    
    for table_name in master_tables:
        csv_file = masters_dir / f"{table_name}.csv"
        
        # CSVファイルの存在確認
        if not csv_file.exists():
            print(f"\n⚠️  {table_name}: CSVファイルが見つかりません")
            print(f"    {csv_file}")
            failed_tables.append((table_name, "CSVファイルが見つかりません"))
            continue
        
        # テーブルを投入
        success, error = load_master_table(client, table_name, csv_file)
        
        if success:
            success_count += 1
        else:
            failed_tables.append((table_name, error))
    
    # 投入完了
    print("\n" + "="*60)
    print("✅ マスタデータ投入完了")
    print(f"  成功: {success_count} / {len(master_tables)}")
    print("="*60)
    
    # データ検証
    print("\n" + "="*60)
    print("🔍 データ検証")
    print("="*60)
    
    for table_name in master_tables:
        verify_data(client, table_name)
    
    # 結果サマリー
    print("\n" + "="*60)
    print("📊 投入結果サマリー")
    print("="*60)
    print(f"✅ 成功: {success_count} テーブル")
    
    if failed_tables:
        print(f"❌ 失敗: {len(failed_tables)} テーブル")
        for table_name, error in failed_tables:
            print(f"  - {table_name}: {error}")
        print("\n⚠️  一部のテーブルの投入に失敗しました")
        print("   エラーを確認して再実行してください")
    else:
        print("🎉 全てのマスタデータが正常に投入されました！")


if __name__ == "__main__":
    main()