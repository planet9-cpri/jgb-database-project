"""
1件だけ修正して動作確認
"""

import logging
from pathlib import Path
from google.cloud import bigquery
from parsers.table_parser import TableParser

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:%(name)s:%(message)s'
)

# BigQuery設定
PROJECT_ID = 'jgb2023'
DATASET_ID = '20251019'
DATA_DIR = Path("G:/マイドライブ/JGBデータ/2023")

# 債券種類からbond_master_idへのマッピング
BOND_TYPE_TO_MASTER_ID = {
    # 通常の利付国債
    '10年': 'BOND_KENSETSU_10Y',
    '10': 'BOND_KENSETSU_10Y',
    '20年': 'BOND_KENSETSU_20Y',
    '20': 'BOND_KENSETSU_20Y',
    '2年': 'BOND_KENSETSU_2Y',
    '2': 'BOND_KENSETSU_2Y',
    '30年': 'BOND_KENSETSU_30Y',
    '30': 'BOND_KENSETSU_30Y',
    '40年': 'BOND_KENSETSU_40Y',
    '40': 'BOND_KENSETSU_40Y',
    '5年': 'BOND_KENSETSU_5Y',
    '5': 'BOND_KENSETSU_5Y',
    # 個人向け国債
    '固定・3年': 'BOND_KOJIN_FIXED_3Y',
    '固定・5年': 'BOND_KOJIN_FIXED_5Y',
    '変動・10年': 'BOND_KOJIN_VARIABLE_10Y',
    # 国庫短期証券（複数パターン対応）
    '短期': 'BOND_TANKI',
    '短期証券': 'BOND_TANKI',
    '短期年': 'BOND_TANKI',  # 念のため
    # 物価連動債
    '物価連動・10年': 'BOND_BUKKA_10Y',
}

def test_single_file():
    """1件だけテスト"""
    
    print("="*70)
    print("🧪 1件テスト：国庫短期証券の抽出とマッピング確認")
    print("="*70)
    print()
    
    # テストファイル
    test_filename = "20230420_令和5年5月10日付（財務省第百三十九号）.txt"
    test_file = DATA_DIR / test_filename
    
    print(f"📄 テストファイル: {test_filename}")
    print()
    
    # ファイル読み込み
    try:
        with open(test_file, 'r', encoding='utf-8') as f:
            content = f.read()
        print(f"✅ ファイル読み込み成功")
    except Exception as e:
        print(f"❌ ファイル読み込みエラー: {e}")
        return
    
    # TableParserで抽出
    print()
    print("-"*70)
    print("🔍 債券種類を抽出中...")
    print("-"*70)
    
    parser = TableParser()
    bond_info = parser.extract_bond_info_from_single(content)
    
    if not bond_info:
        print("❌ 債券情報を抽出できませんでした")
        return
    
    bond_type = bond_info.bond_type
    print(f"✅ 抽出成功: bond_type = '{bond_type}'")
    print()
    
    # マッピング確認
    print("-"*70)
    print("🗺️ bond_master_idへのマッピング")
    print("-"*70)
    
    if bond_type in BOND_TYPE_TO_MASTER_ID:
        bond_master_id = BOND_TYPE_TO_MASTER_ID[bond_type]
        print(f"✅ マッピング成功")
        print(f"  債券種類: {bond_type}")
        print(f"  ↓")
        print(f"  bond_master_id: {bond_master_id}")
        print()
        
        # bonds_masterに存在するか確認
        print("-"*70)
        print("🔍 bonds_masterに存在するか確認")
        print("-"*70)
        
        client = bigquery.Client(project=PROJECT_ID)
        query = f"""
        SELECT bond_id, bond_name
        FROM `{PROJECT_ID}.{DATASET_ID}.bonds_master`
        WHERE bond_id = '{bond_master_id}'
        """
        
        results = list(client.query(query).result())
        if results:
            print(f"✅ bonds_masterに存在します")
            for row in results:
                print(f"  bond_id: {row.bond_id}")
                print(f"  bond_name: {row.bond_name}")
        else:
            print(f"❌ bonds_masterに存在しません")
            print(f"  bonds_masterに追加が必要です")
    else:
        print(f"❌ マッピングに存在しない債券種類: {bond_type}")
        print(f"  BOND_TYPE_TO_MASTER_IDに追加が必要です")
    
    print()
    print("="*70)
    print("✅ テスト完了")
    print("="*70)

if __name__ == '__main__':
    test_single_file()