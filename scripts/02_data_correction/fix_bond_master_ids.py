"""
既存155件のbond_master_idを修正
BOND_001 → 正しいbond_master_idに更新

Day 4完了用スクリプト（特殊債券対応）
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# ロギング設定（parsersのINFOログを抑制）
logging.basicConfig(level=logging.WARNING)

# パスの設定
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from parsers.issue_extractor import IssueExtractor
from parsers.table_parser import TableParser

# 設定
PROJECT_ID = "jgb2023"
DATASET_ID = "20251019"
BOND_MASTER_TABLE = "bonds_master"
DATA_DIR = Path(r"G:\マイドライブ\JGBデータ")

# 債券種類とbond_master_idのマッピング（全角・半角両対応 + 特殊債券対応）
BOND_TYPE_TO_MASTER_ID = {
    # 通常の利付国債
    '２年': 'BOND_KENSETSU_2Y',
    '2年': 'BOND_KENSETSU_2Y',
    '５年': 'BOND_KENSETSU_5Y',
    '5年': 'BOND_KENSETSU_5Y',
    '10年': 'BOND_KENSETSU_10Y',
    '20年': 'BOND_KENSETSU_20Y',
    '30年': 'BOND_KENSETSU_30Y',
    '40年': 'BOND_KENSETSU_40Y',
    # 個人向け国債
    '固定・３年': 'BOND_KOJIN_FIXED_3Y',
    '固定・3年': 'BOND_KOJIN_FIXED_3Y',
    '固定・５年': 'BOND_KOJIN_FIXED_5Y',
    '固定・5年': 'BOND_KOJIN_FIXED_5Y',
    '変動・10年': 'BOND_KOJIN_VARIABLE_10Y',
    # 国庫短期証券
    '短期': 'BOND_TANKI',
    '短期証券': 'BOND_TANKI',
    '短期年': 'BOND_TANKI',  # 抽出ミス対応（念のため）
    # 物価連動債
    '物価連動・10年': 'BOND_BUKKA_10Y',
}

# bonds_masterに追加する特殊債券マスター
SPECIAL_BOND_MASTERS = {
    'BOND_KOJIN_FIXED_3Y': {
        'bond_id': 'BOND_KOJIN_FIXED_3Y',
        'bond_name': '個人向け利付国庫債券（固定・3年）',
        'bond_type': '個人向け国債',
        'maturity_years': 3.0,
        'maturity_type': '固定・3年',
        'issue_method': '個人向け',
        'interest_type': '固定利付',
        'interest_payment': '年2回',
        'min_denomination': 10000,
        'description': '個人向け国債・固定金利3年物',
        'is_active': True
    },
    'BOND_KOJIN_FIXED_5Y': {
        'bond_id': 'BOND_KOJIN_FIXED_5Y',
        'bond_name': '個人向け利付国庫債券（固定・5年）',
        'bond_type': '個人向け国債',
        'maturity_years': 5.0,
        'maturity_type': '固定・5年',
        'issue_method': '個人向け',
        'interest_type': '固定利付',
        'interest_payment': '年2回',
        'min_denomination': 10000,
        'description': '個人向け国債・固定金利5年物',
        'is_active': True
    },
    'BOND_KOJIN_VARIABLE_10Y': {
        'bond_id': 'BOND_KOJIN_VARIABLE_10Y',
        'bond_name': '個人向け利付国庫債券（変動・10年）',
        'bond_type': '個人向け国債',
        'maturity_years': 10.0,
        'maturity_type': '変動・10年',
        'issue_method': '個人向け',
        'interest_type': '変動利付',
        'interest_payment': '年2回',
        'min_denomination': 10000,
        'description': '個人向け国債・変動金利10年物',
        'is_active': True
    },
    'BOND_TANKI': {
        'bond_id': 'BOND_TANKI',
        'bond_name': '国庫短期証券',
        'bond_type': '短期国債',
        'maturity_years': 0.25,
        'maturity_type': '短期',
        'issue_method': '割引発行',
        'interest_type': '割引',
        'interest_payment': 'なし',
        'min_denomination': 50000,
        'description': '国庫短期証券（割引短期国債）',
        'is_active': True
    },
    'BOND_BUKKA_10Y': {
        'bond_id': 'BOND_BUKKA_10Y',
        'bond_name': '利付国庫債券（物価連動・10年）',
        'bond_type': '物価連動国債',
        'maturity_years': 10.0,
        'maturity_type': '物価連動・10年',
        'issue_method': '物価連動',
        'interest_type': '利付',
        'interest_payment': '年2回',
        'min_denomination': 100000,
        'description': '物価連動国債・10年物',
        'is_active': True
    }
}


def get_bond_001_files(client: bigquery.Client):
    """BOND_001を使用している告示のファイル名を取得"""
    query = f"""
    SELECT DISTINCT 
        a.announcement_id,
        a.source_file
    FROM `{PROJECT_ID}.{DATASET_ID}.announcements` a
    INNER JOIN `{PROJECT_ID}.{DATASET_ID}.bond_issuances` bi
        ON a.announcement_id = bi.announcement_id
    WHERE bi.bond_master_id = 'BOND_001'
    ORDER BY a.announcement_id
    """
    
    results = client.query(query).result()
    files = []
    for row in results:
        files.append({
            'announcement_id': row.announcement_id,
            'source_file': row.source_file
        })
    
    return files


def ensure_special_bonds_in_master(client: bigquery.Client):
    """bonds_masterに特殊債券が存在することを確認し、なければ追加"""
    print("\n📋 bonds_masterに特殊債券を確認中...")
    
    # 既存のbond_idを取得
    query = f"""
    SELECT bond_id
    FROM `{PROJECT_ID}.{DATASET_ID}.{BOND_MASTER_TABLE}`
    """
    
    existing_ids = set()
    try:
        results = client.query(query).result()
        for row in results:
            existing_ids.add(row.bond_id)
    except Exception as e:
        print(f"⚠️ bonds_master確認エラー: {e}")
    
    # 不足している特殊債券を追加
    missing_bonds = []
    for bond_id, bond_data in SPECIAL_BOND_MASTERS.items():
        if bond_id not in existing_ids:
            missing_bonds.append(bond_data)
    
    if missing_bonds:
        print(f"📝 bonds_masterに{len(missing_bonds)}種類の特殊債券を追加します")
        for bond in missing_bonds:
            print(f"  - {bond['bond_name']}")
        
        # タイムスタンプを追加
        rows = []
        for bond in missing_bonds:
            bond_copy = bond.copy()
            bond_copy['created_at'] = datetime.now().isoformat()
            bond_copy['updated_at'] = datetime.now().isoformat()
            rows.append(bond_copy)
        
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{BOND_MASTER_TABLE}"
        errors = client.insert_rows_json(table_ref, rows)
        
        if errors:
            print(f"❌ bonds_master投入エラー:")
            for error in errors:
                print(f"  {error}")
            raise Exception("bonds_master投入失敗")
        else:
            print(f"✅ bonds_masterに{len(rows)}件追加しました")
    else:
        print("✅ 特殊債券はすべて登録済みです")


def extract_bond_type_from_file(filepath: str) -> str:
    """ファイルから債券種類を抽出"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            notice_text = f.read()
        
        # まず別表形式（複数銘柄）で試行
        extractor = IssueExtractor(notice_text)
        issues = extractor.extract_issues()
        
        if issues and len(issues) > 0:
            # 最初の銘柄の種類を返す
            return issues[0]['bond_type']
        
        # 別表がない場合、単一銘柄として試行
        print(f"  ℹ️ 別表なし。単一銘柄モードで試行...")
        table_parser = TableParser()
        bond_issuance = table_parser.extract_bond_info_from_single(notice_text)
        
        if bond_issuance:
            # BondIssuanceオブジェクトからbond_typeを取得
            bond_type = bond_issuance.bond_type
            
            # 「年」が含まれていない場合は追加
            if '年' not in bond_type:
                bond_type = f"{bond_type}年"
            
            print(f"  ✅ 単一銘柄抽出成功: {bond_type}")
            return bond_type
        
        return None
    
    except Exception as e:
        print(f"  ❌ 抽出エラー: {e}")
        import traceback
        traceback.print_exc()
        return None


def update_bond_master_id(client: bigquery.Client, announcement_id: str, new_bond_master_id: str):
    """bond_master_idを更新"""
    query = f"""
    UPDATE `{PROJECT_ID}.{DATASET_ID}.bond_issuances`
    SET 
        bond_master_id = @new_bond_master_id,
        updated_at = CURRENT_TIMESTAMP()
    WHERE announcement_id = @announcement_id
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("new_bond_master_id", "STRING", new_bond_master_id),
            bigquery.ScalarQueryParameter("announcement_id", "STRING", announcement_id)
        ]
    )
    
    client.query(query, job_config=job_config).result()


def main():
    """メイン実行"""
    print("=" * 70)
    print("🔧 BOND_001修正スクリプト（特殊債券対応）")
    print("=" * 70)
    
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
    
    # BOND_001を使用しているファイルを取得
    print("\n📋 BOND_001を使用している告示を取得中...")
    files = get_bond_001_files(client)
    print(f"✅ {len(files)}件の告示を特定しました")
    
    if not files:
        print("修正対象がありません")
        return 0
    
    # bonds_masterに特殊債券を追加
    ensure_special_bonds_in_master(client)
    
    # 確認
    print(f"\n⚠️ {len(files)}件のbond_master_idを更新します")
    response = input("続行しますか？ (yes/no): ")
    if response.lower() != 'yes':
        print("キャンセルしました")
        return 0
    
    # 各ファイルを処理
    print("\n" + "=" * 70)
    print("🚀 処理開始")
    print("=" * 70)
    
    success_count = 0
    failed_count = 0
    failed_files = []
    
    for i, file_info in enumerate(files, 1):
        announcement_id = file_info['announcement_id']
        source_file = file_info['source_file']
        
        print(f"\n[{i}/{len(files)}] {source_file}")
        print(f"  announcement_id: {announcement_id}")
        
        # ファイルパスを構築（年度ベース）
        issue_date = source_file[:8]  # YYYYMMDD
        year = int(issue_date[:4])
        month = int(issue_date[4:6])
        
        if month >= 4:
            fiscal_year = year
        else:
            fiscal_year = year - 1
        
        filepath = DATA_DIR / str(fiscal_year) / source_file
        
        if not filepath.exists():
            print(f"  ❌ ファイルが見つかりません: {filepath}")
            failed_count += 1
            failed_files.append({'file': source_file, 'reason': 'ファイル未発見'})
            continue
        
        # 債券種類を抽出
        bond_type = extract_bond_type_from_file(str(filepath))
        
        if not bond_type:
            print(f"  ⚠️ 債券種類を抽出できませんでした")
            failed_count += 1
            failed_files.append({'file': source_file, 'reason': '抽出失敗'})
            continue
        
        # bond_master_idを取得
        bond_master_id = BOND_TYPE_TO_MASTER_ID.get(bond_type)
        
        if not bond_master_id:
            print(f"  ⚠️ 未知の債券種類: {bond_type}")
            failed_count += 1
            failed_files.append({'file': source_file, 'reason': f'未知の種類: {bond_type}'})
            continue
        
        print(f"  債券種類: {bond_type} → {bond_master_id}")
        
        # BigQueryを更新
        try:
            update_bond_master_id(client, announcement_id, bond_master_id)
            print(f"  ✅ 更新成功")
            success_count += 1
        except Exception as e:
            print(f"  ❌ 更新エラー: {e}")
            failed_count += 1
            failed_files.append({'file': source_file, 'reason': f'DB更新エラー: {e}'})
    
    # 結果サマリー
    print("\n" + "=" * 70)
    print("📊 処理結果")
    print("=" * 70)
    print(f"総件数: {len(files)}")
    print(f"✅ 成功: {success_count}")
    print(f"❌ 失敗: {failed_count}")
    
    # 失敗したファイルの詳細
    if failed_files:
        print(f"\n【失敗したファイル】")
        for item in failed_files:
            print(f"  - {item['file']}")
            print(f"    理由: {item['reason']}")
    
    # 最終確認
    if success_count > 0:
        print("\n" + "=" * 70)
        print("🔍 更新後の確認")
        print("=" * 70)
        
        query = f"""
        SELECT 
            bm.bond_name,
            COUNT(*) as count
        FROM `{PROJECT_ID}.{DATASET_ID}.bond_issuances` bi
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.{BOND_MASTER_TABLE}` bm
            ON bi.bond_master_id = bm.bond_id
        GROUP BY bm.bond_name
        ORDER BY bm.bond_name
        """
        
        results = client.query(query).result()
        print("\n【種類別集計】")
        for row in results:
            bond_name = row.bond_name or "不明"
            print(f"  {bond_name}: {row.count}件")
    
    if failed_count == 0:
        print("\n🎉 すべての修正が完了しました！")
        print("=" * 70)
        print("Day 4 完了 🎊")
        print("=" * 70)
        return 0
    else:
        print(f"\n⚠️ {failed_count}件の修正に失敗しました")
        print("失敗したファイルを確認して、個別に対応してください")
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)