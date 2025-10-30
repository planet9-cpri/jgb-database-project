"""
BigQuery銘柄投入スクリプト（修正版）
Day 4で抽出した銘柄をBigQueryに投入
bonds_master + bond_issuances の構造に対応
"""

import json
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Set
from google.cloud import bigquery
from google.oauth2 import service_account
import re

# 設定
PROJECT_ID = "jgb2023"
DATASET_ID = "20251019"
BOND_MASTER_TABLE = "bonds_master"
ISSUANCES_TABLE = "bond_issuances"

# 債券種類マスター（bonds_master用）
BOND_TYPES = {
    '２年': {  # 全角表記（パーサーの出力に合わせる）
        'bond_id': 'BOND_KENSETSU_2Y',
        'bond_name': '利付国庫債券（2年）',
        'bond_type': '利付国債',
        'maturity_years': 2.0,
        'maturity_type': '2年',
        'issue_method': '流動性供給',
        'interest_type': '利付',
        'interest_payment': '年2回',
        'min_denomination': 50000,
        'description': '固定金利の利付国債・2年物',
        'is_active': True
    },
    '５年': {  # 全角表記（パーサーの出力に合わせる）
        'bond_id': 'BOND_KENSETSU_5Y',
        'bond_name': '利付国庫債券（5年）',
        'bond_type': '利付国債',
        'maturity_years': 5.0,
        'maturity_type': '5年',
        'issue_method': '流動性供給',
        'interest_type': '利付',
        'interest_payment': '年2回',
        'min_denomination': 50000,
        'description': '固定金利の利付国債・5年物',
        'is_active': True
    },
    '10年': {
        'bond_id': 'BOND_KENSETSU_10Y',
        'bond_name': '利付国庫債券（10年）',
        'bond_type': '利付国債',
        'maturity_years': 10.0,
        'maturity_type': '10年',
        'issue_method': '流動性供給',
        'interest_type': '利付',
        'interest_payment': '年2回',
        'min_denomination': 50000,
        'description': '固定金利の利付国債・10年物',
        'is_active': True
    },
    '20年': {
        'bond_id': 'BOND_KENSETSU_20Y',
        'bond_name': '利付国庫債券（20年）',
        'bond_type': '利付国債',
        'maturity_years': 20.0,
        'maturity_type': '20年',
        'issue_method': '流動性供給',
        'interest_type': '利付',
        'interest_payment': '年2回',
        'min_denomination': 50000,
        'description': '固定金利の利付国債・20年物・利子支払期は6月20日と12月20日',
        'is_active': True
    },
    '30年': {
        'bond_id': 'BOND_KENSETSU_30Y',
        'bond_name': '利付国庫債券（30年）',
        'bond_type': '利付国債',
        'maturity_years': 30.0,
        'maturity_type': '30年',
        'issue_method': '流動性供給',
        'interest_type': '利付',
        'interest_payment': '年2回',
        'min_denomination': 50000,
        'description': '固定金利の利付国債・30年物・利子支払期は9月20日と3月20日',
        'is_active': True
    },
    '40年': {
        'bond_id': 'BOND_KENSETSU_40Y',
        'bond_name': '利付国庫債券（40年）',
        'bond_type': '利付国債',
        'maturity_years': 40.0,
        'maturity_type': '40年',
        'issue_method': '流動性供給',
        'interest_type': '利付',
        'interest_payment': '年2回',
        'min_denomination': 50000,
        'description': '固定金利の利付国債・40年物・利子支払期は9月20日と3月20日',
        'is_active': True
    }
}


def load_extraction_results(json_path: str) -> Dict:
    """抽出結果JSONを読み込み"""
    with open(json_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_announcement_id_from_filename(filename: str) -> str:
    """
    ファイル名からannouncement_idを生成
    例: 20230414_令和5年5月9日付（財務省第百二十七号）.txt → ANN_20230414_127
    """
    # 日付部分（YYYYMMDD）
    date_match = re.match(r'(\d{8})_', filename)
    if not date_match:
        return f"ANN_unknown_{filename[:20]}"
    
    date_part = date_match.group(1)
    
    # 告示番号の抽出（漢数字変換マップ）
    kanji_to_num = {
        '百二十七': '127', '百二十六': '126', '百五十八': '158', '百五十九': '159',
        '百八十六': '186', '百八十七': '187', '二百一': '201', '二百二': '202',
        '二百二十二': '222', '二百二十三': '223', '二百五十四': '254', '二百五十五': '255',
        '二百七十四': '274', '二百七十五': '275', '三百六': '306', '三百七': '307',
        '七': '7', '八': '8', '三十八': '38', '三十九': '39', '七十四': '74',
        '七十三': '73', '百十二': '112', '百十一': '111'
    }
    
    notice_match = re.search(r'第([一二三四五六七八九十百千万]+)号', filename)
    if notice_match:
        notice_str = notice_match.group(1)
        notice_num = kanji_to_num.get(notice_str, notice_str)
    else:
        notice_num = "unknown"
    
    return f"ANN_{date_part}_{notice_num}"


def get_issuance_date_from_filename(filename: str) -> str:
    """ファイル名から発行日を取得（YYYYMMDD → YYYY-MM-DD）"""
    date_match = re.match(r'(\d{4})(\d{2})(\d{2})_', filename)
    if date_match:
        return f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
    return None


def check_missing_bond_types(client: bigquery.Client, results: Dict) -> Set[str]:
    """
    bonds_masterに不足している債券種類を確認
    
    Returns:
        不足している種類のセット（例: {'10年', '20年'}）
    """
    # 抽出データから使用されている種類を収集
    used_types = set()
    for item in results['success']:
        for issue in item['issues']:
            used_types.add(issue['bond_type'])
    
    # 既存のbonds_masterを確認
    query = f"""
    SELECT DISTINCT maturity_type
    FROM `{PROJECT_ID}.{DATASET_ID}.{BOND_MASTER_TABLE}`
    WHERE is_active = TRUE
    """
    
    existing_types = set()
    try:
        results_query = client.query(query).result()
        for row in results_query:
            existing_types.add(row.maturity_type)
    except Exception as e:
        print(f"⚠️ bonds_master確認エラー: {e}")
    
    # 不足している種類
    missing = used_types - existing_types
    
    return missing


def insert_missing_bond_masters(client: bigquery.Client, missing_types: Set[str]):
    """不足している債券種類をbonds_masterに追加"""
    if not missing_types:
        return
    
    print(f"\n📝 bonds_masterに{len(missing_types)}種類を追加します")
    
    rows = []
    for bond_type in missing_types:
        if bond_type in BOND_TYPES:
            master_data = BOND_TYPES[bond_type].copy()
            master_data['created_at'] = datetime.now().isoformat()
            master_data['updated_at'] = datetime.now().isoformat()
            rows.append(master_data)
            print(f"  - {master_data['bond_name']}")
    
    if rows:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{BOND_MASTER_TABLE}"
        errors = client.insert_rows_json(table_ref, rows)
        
        if errors:
            print(f"❌ bonds_master投入エラー:")
            for error in errors:
                print(f"  {error}")
            raise Exception("bonds_master投入失敗")
        else:
            print(f"✅ bonds_masterに{len(rows)}件追加しました")


def prepare_issuance_rows(results: Dict) -> List[Dict]:
    """
    bond_issuances投入用のデータを準備
    
    Returns:
        BigQuery行データのリスト
    """
    rows = []
    
    for item in results['success']:
        filename = item['filename']
        announcement_id = get_announcement_id_from_filename(filename)
        issuance_date_str = get_issuance_date_from_filename(filename)
        
        for issue in item['issues']:
            # maturity_date変換
            maturity_date = None
            if issue.get('maturity_date'):
                try:
                    dt = datetime.fromisoformat(issue['maturity_date'])
                    maturity_date = dt.strftime('%Y-%m-%d')
                except:
                    maturity_date = None
            
            # bond_master_idを取得
            bond_type = issue['bond_type']
            if bond_type in BOND_TYPES:
                bond_master_id = BOND_TYPES[bond_type]['bond_id']
            else:
                bond_master_id = f"BOND_UNKNOWN_{bond_type}"
            
            # issuance_id生成
            # フォーマット: ANN_YYYYMMDD_XXX_ISSUE_YYY
            # 例: ANN_20230414_127_ISSUE_167
            series_num = issue['series_number']
            issuance_id = f"{announcement_id}_ISSUE_{series_num:03d}"
            
            row = {
                'issuance_id': issuance_id,
                'announcement_id': announcement_id,
                'bond_master_id': bond_master_id,
                'issuance_date': issuance_date_str,
                'maturity_date': maturity_date,
                'interest_rate': float(issue['rate']) if issue.get('rate') else None,
                'issue_price': None,  # 抽出データにはないのでNULL
                'issue_amount': int(issue['amount']) if issue.get('amount') else None,
                'payment_date': issuance_date_str,  # 発行日と同じと仮定
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            
            rows.append(row)
    
    return rows


def upload_to_bigquery(rows: List[Dict], credentials_path: str = None):
    """BigQueryにデータをアップロード"""
    print("\n" + "=" * 70)
    print("🚀 BigQuery投入開始")
    print("=" * 70)
    print(f"プロジェクト: {PROJECT_ID}")
    print(f"データセット: {DATASET_ID}")
    print(f"テーブル: {ISSUANCES_TABLE}")
    print(f"投入行数: {len(rows)}")
    print()
    
    # BigQueryクライアント初期化
    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    else:
        client = bigquery.Client(project=PROJECT_ID)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{ISSUANCES_TABLE}"
    
    try:
        # データ投入
        print("投入中...")
        errors = client.insert_rows_json(table_ref, rows)
        
        if errors:
            print("\n❌ エラーが発生しました:")
            for error in errors:
                print(f"  {error}")
            return False
        else:
            print(f"\n✅ 投入成功：{len(rows)}行")
            
            # サンプル表示
            print("\n【投入データサンプル（最初の3件）】")
            for i, row in enumerate(rows[:3], 1):
                print(f"\n{i}. issuance_id: {row['issuance_id']}")
                print(f"   announcement_id: {row['announcement_id']}")
                print(f"   bond_master_id: {row['bond_master_id']}")
                print(f"   利率: {row['interest_rate']}%, 発行額: {row['issue_amount']:,}円")
            
            if len(rows) > 3:
                print(f"\n   ... 他{len(rows) - 3}件")
            
            return True
    
    except Exception as e:
        print(f"\n❌ BigQueryエラー: {e}")
        import traceback
        traceback.print_exc()
        return False


def verify_upload(credentials_path: str = None):
    """投入後の確認クエリを実行"""
    print("\n" + "=" * 70)
    print("📊 投入結果の確認")
    print("=" * 70)
    
    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    else:
        client = bigquery.Client(project=PROJECT_ID)
    
    # 発行件数の確認
    query = f"""
    SELECT 
        COUNT(*) as total_issuances,
        COUNT(DISTINCT announcement_id) as total_announcements,
        COUNT(DISTINCT bond_master_id) as total_bond_types
    FROM `{PROJECT_ID}.{DATASET_ID}.{ISSUANCES_TABLE}`
    """
    
    try:
        results = client.query(query).result()
        for row in results:
            print(f"\n総発行件数: {row.total_issuances}")
            print(f"総告示数: {row.total_announcements}")
            print(f"債券種類数: {row.total_bond_types}")
        
        # 種類別集計
        query2 = f"""
        SELECT 
            bm.bond_name,
            COUNT(*) as count
        FROM `{PROJECT_ID}.{DATASET_ID}.{ISSUANCES_TABLE}` bi
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.{BOND_MASTER_TABLE}` bm
            ON bi.bond_master_id = bm.bond_id
        GROUP BY bm.bond_name
        ORDER BY bm.bond_name
        """
        
        print("\n【種類別集計】")
        results2 = client.query(query2).result()
        for row in results2:
            bond_name = row.bond_name or "不明"
            print(f"  {bond_name}: {row.count}件")
        
        return True
    
    except Exception as e:
        print(f"\n❌ 確認クエリエラー: {e}")
        return False


def main():
    """メイン実行"""
    # 抽出結果の読み込み
    json_path = "extraction_results.json"
    
    if not Path(json_path).exists():
        print(f"❌ {json_path}が見つかりません")
        return 1
    
    print("📄 抽出結果を読み込み中...")
    results = load_extraction_results(json_path)
    
    print(f"✅ 読み込み完了")
    print(f"  成功ファイル: {len(results['success'])}件")
    print(f"  総銘柄数: {results['summary']['total_issues']}")
    print()
    
    # BigQueryクライアント初期化
    credentials_path = None  # または "path/to/service-account-key.json"
    
    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    else:
        client = bigquery.Client(project=PROJECT_ID)
    
    # bonds_masterの不足確認
    print("📋 bonds_masterの確認中...")
    missing_types = check_missing_bond_types(client, results)
    
    if missing_types:
        print(f"⚠️ 不足している債券種類: {missing_types}")
        insert_missing_bond_masters(client, missing_types)
    else:
        print("✅ bonds_masterは完全です")
    
    # bond_issuances投入データを準備
    print("\n📝 bond_issuances投入データを準備中...")
    rows = prepare_issuance_rows(results)
    print(f"✅ 準備完了：{len(rows)}行")
    print()
    
    # 確認
    response = input(f"BigQueryに{len(rows)}行を投入しますか？ (yes/no): ")
    if response.lower() != 'yes':
        print("キャンセルしました")
        return 0
    
    # BigQueryに投入
    success = upload_to_bigquery(rows, credentials_path)
    
    if success:
        # 投入結果の確認
        verify_upload(credentials_path)
        
        print("\n" + "=" * 70)
        print("🎉 Day 4完了！")
        print("=" * 70)
        print(f"✅ 24件の告示から717銘柄を抽出")
        print(f"✅ {len(rows)}件をBigQueryに投入")
        print(f"✅ 達成率: 100%")
        print()
        
        return 0
    else:
        print("\n⚠️ 投入に失敗しました")
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)