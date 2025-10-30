"""
データ投入スクリプト（20251025データセット用）

完全修正版：
- re.DOTALL フラグの追加（改行対応）
- 全角数字対応
- より柔軟な正規表現パターン
"""

import os
import sys
import re
from datetime import datetime, date
import uuid

# プロジェクトルートをパスに追加
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from google.cloud import bigquery

# パーサーのインポート
try:
    from parsers.table_parser import TableParser
    HAS_PARSERS = True
except ImportError:
    print("⚠️  警告: parsersモジュールが見つかりません")
    HAS_PARSERS = False

# BigQuery設定
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json"
PROJECT_ID = 'jgb2023'
DATASET_ID = '20251025'

client = bigquery.Client(project=PROJECT_ID)

# データディレクトリ
DATA_DIR = r"G:\マイドライブ\JGBデータ\2023"

# 発行根拠法令パターン（2023年度用・優先順位最適化版）
# ⭐ 重要：より具体的なパターンを先にチェック
LEGAL_BASIS_PATTERNS = {
    # 最優先：財投債（第62条を含む）
    '財投債': {
        'category': '財投債',
        'sub_category': '財投債',
        'patterns': [
            r'特別会計に関する法律[^条]*第62条第[1１一]項',
            r'特別会計に関する法律[^条]*第六十二条第[1１一]項'
        ]
    },
    # 優先：特例債
    '特例債': {
        'category': '特例債',
        'sub_category': '特例公債',
        'patterns': [
            r'財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律[^条]*第[2２二]条第[1１一]項',
            r'平成24年法律第101号[^条]*第[2２二]条第[1１一]項'
        ]
    },
    # 年金特例債
    '年金特例債': {
        'category': '年金特例債',
        'sub_category': 'つなぎ公債',
        'patterns': [
            r'財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律[^条]*第[3３三]条第[1１一]項',
            r'平成24年法律第101号[^条]*第[3３三]条第[1１一]項'
        ]
    },
    # 4条債
    '4条債': {
        'category': '4条債',
        'sub_category': '建設国債',
        'patterns': [
            r'財政法[^条]*第[4４四]条第[1１一]項',
            r'財政法[^条]*第四条第[1１一]項'
        ]
    },
    # GX経済移行債
    'GX経済移行債': {
        'category': 'GX経済移行債',
        'sub_category': 'つなぎ公債',
        'patterns': [
            r'脱炭素成長型経済構造への円滑な移行の推進に関する法律[^条]*第[7７七]条第[1１一]項',
            r'令和[5５]年法律第32号[^条]*第[7７七]条第[1１一]項'
        ]
    },
    # 復興債
    '復興債': {
        'category': '復興債',
        'sub_category': 'つなぎ公債',
        'patterns': [
            r'東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法[^条]*第69条第[4４四]項',
            r'東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法[^条]*第六十九条第[4４四]項'
        ]
    },
    # 前倒債
    '前倒債': {
        'category': '借換債',
        'sub_category': '前倒債',
        'patterns': [
            r'特別会計に関する法律[^条]*第47条第[1１一]項',
            r'特別会計に関する法律[^条]*第四十七条第[1１一]項'
        ]
    },
    # 最後：借換債（最も一般的なため最後にチェック）
    '借換債': {
        'category': '借換債',
        'sub_category': '借換債',
        'patterns': [
            r'特別会計に関する法律[^条]*第46条第[1１一]項',
            r'特別会計に関する法律[^条]*第四十六条第[1１一]項'
        ]
    }
}


def parse_date_string(date_value):
    """日付文字列をdateオブジェクトに変換"""
    if date_value is None:
        return None
    if isinstance(date_value, date):
        return date_value
    if isinstance(date_value, str):
        try:
            return datetime.strptime(date_value, '%Y-%m-%d').date()
        except:
            try:
                return datetime.strptime(date_value, '%Y/%m/%d').date()
            except:
                return None
    return None


def extract_legal_bases(text):
    """発行根拠法令を抽出（複数対応・「及び」対応）"""
    results = []
    
    # ⭐ 重要：「及び」で分割される複数の条項を個別にチェック
    for basis_name, config in LEGAL_BASIS_PATTERNS.items():
        for pattern in config['patterns']:
            # re.DOTALL フラグを追加（改行をマッチ）
            matches = re.finditer(pattern, text, re.IGNORECASE | re.DOTALL)
            
            for match in matches:
                full_text = match.group(0)
                
                # 重複チェック（同じ法令根拠を2回追加しない）
                if not any(r['basis'] == basis_name for r in results):
                    results.append({
                        'basis': basis_name,
                        'category': config['category'],
                        'sub_category': config['sub_category'],
                        'full': full_text
                    })
    
    return results


def extract_series_number(text):
    """回号を抽出"""
    match = re.search(r'第(\d+)回', text)
    if match:
        return f"第{match.group(1)}回"
    return None


def calculate_maturity_period(issuance_date, maturity_date):
    """償還期間を計算"""
    issuance_date = parse_date_string(issuance_date)
    maturity_date = parse_date_string(maturity_date)
    
    if not issuance_date or not maturity_date:
        return None, None, None
    
    delta = maturity_date - issuance_date
    days = delta.days
    months = days / 30.0
    years = days / 365.0
    
    return days, months, years


def determine_bond_master_id(bond_name, maturity_days):
    """債券名と償還期間から適切なbond_master_idを決定"""
    # ⭐ 重要：債券名が存在しない場合
    if not bond_name:
        return None
    
    # ⭐ 修正：債券名を優先（日付がNULLでも対応可能）
    mapping = {
        '2年': 'BOND_002Y',
        '5年': 'BOND_005Y',
        '10年': 'BOND_010Y',
        '20年': 'BOND_020Y',
        '30年': 'BOND_030Y',
        '40年': 'BOND_040Y',
        '固定・3年': 'BOND_KOJIN_FIXED_3Y',
        '固定・5年': 'BOND_KOJIN_FIXED_5Y',
        '変動・10年': 'BOND_KOJIN_VARIABLE_10Y',
        '物価連動': 'BOND_BUKKA_10Y'
    }
    
    # 債券名から直接判定（最優先）
    for key, value in mapping.items():
        if key in bond_name:
            return value
    
    # クライメート・トランジション債
    if 'クライメート' in bond_name or 'トランジション' in bond_name:
        if '10年' in bond_name:
            return 'BOND_GX_10Y'
        elif '5年' in bond_name:
            return 'BOND_GX_5Y'
    
    # 短期債（償還期間が必要）
    if '短期' in bond_name or 'TANKI' in bond_name:
        if maturity_days and maturity_days >= 270:
            return 'BOND_TANKI_1Y'
        elif maturity_days:
            return 'BOND_TANKI_6M'
        else:
            # 日付不明の場合はデフォルトで6ヶ月
            return 'BOND_TANKI_6M'
    
    return None


def get_issuance_attribute(issuance, attr_name):
    """BondIssuanceオブジェクトから属性を安全に取得"""
    possible_names = {
        'issuance_date': ['issuance_date', 'issue_date', 'issueDate'],
        'maturity_date': ['maturity_date', 'maturityDate'],
        'payment_date': ['payment_date', 'paymentDate'],
        'issue_amount': ['issue_amount', 'issueAmount', 'face_value_individual'],
        'issue_price': ['issue_price', 'issuePrice'],
        'interest_rate': ['interest_rate', 'interestRate'],
        'series_number': ['series_number', 'seriesNumber'],
        'bond_name': ['bond_name', 'bondName']
    }
    
    if attr_name in possible_names:
        for name in possible_names[attr_name]:
            if hasattr(issuance, name):
                return getattr(issuance, name)
    
    if hasattr(issuance, attr_name):
        return getattr(issuance, attr_name)
    
    return None


def process_announcement_file(file_path):
    """1つの告示ファイルを処理"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    file_name = os.path.basename(file_path)
    announcement_id = file_name.replace('.txt', '')
    
    # 日付抽出
    date_match = re.match(r'(\d{8})_', file_name)
    if date_match:
        date_str = date_match.group(1)
        kanpo_date = datetime.strptime(date_str, '%Y%m%d').date()
    else:
        kanpo_date = None
    
    announcement_number = extract_series_number(content)
    
    announcement_data = {
        'announcement_id': announcement_id,
        'kanpo_date': kanpo_date.isoformat() if kanpo_date else None,
        'announcement_number': announcement_number,
        'source_file': file_name,
        'created_at': datetime.now().isoformat(),
        'updated_at': datetime.now().isoformat()
    }
    
    # 発行根拠法令を抽出
    legal_bases = extract_legal_bases(content)
    
    if legal_bases:
        basis_summary = ', '.join([f"{lb['basis']}({lb['category']})" for lb in legal_bases])
        print(f"  → 発行根拠: {basis_summary}")
    else:
        print(f"  ⚠️  発行根拠法令が抽出できませんでした")
    
    # 発行情報を抽出
    all_issuances = []
    if HAS_PARSERS:
        parser = TableParser()
        try:
            issuances = parser.extract_bond_info(content)
        except:
            issuances = [parser.extract_bond_info_from_single(content)]
        
        for issuance in issuances:
            if not issuance:
                continue
            
            issuance_date = get_issuance_attribute(issuance, 'issuance_date')
            maturity_date = get_issuance_attribute(issuance, 'maturity_date')
            payment_date = get_issuance_attribute(issuance, 'payment_date')
            issue_amount = get_issuance_attribute(issuance, 'issue_amount')
            issue_price = get_issuance_attribute(issuance, 'issue_price')
            interest_rate = get_issuance_attribute(issuance, 'interest_rate')
            series_number = get_issuance_attribute(issuance, 'series_number')
            bond_name = get_issuance_attribute(issuance, 'bond_name')
            
            days, months, years = calculate_maturity_period(issuance_date, maturity_date)
            bond_master_id = determine_bond_master_id(bond_name or '', days)
            
            issuance_date_str = parse_date_string(issuance_date)
            issuance_date_str = issuance_date_str.isoformat() if issuance_date_str else None
            
            maturity_date_str = parse_date_string(maturity_date)
            maturity_date_str = maturity_date_str.isoformat() if maturity_date_str else None
            
            payment_date_str = parse_date_string(payment_date)
            payment_date_str = payment_date_str.isoformat() if payment_date_str else None
            
            issuance_data = {
                'issuance_id': str(uuid.uuid4()),
                'announcement_id': announcement_id,
                'bond_master_id': bond_master_id,
                'series_number': series_number or extract_series_number(content),
                'issuance_date': issuance_date_str,
                'maturity_date': maturity_date_str,
                'payment_date': payment_date_str,
                'maturity_period_days': days,
                'maturity_period_months': months,
                'maturity_period_years': years,
                'issue_amount': issue_amount,
                'issue_price': issue_price,
                'interest_rate': interest_rate,
                'legal_basis': legal_bases[0]['basis'] if legal_bases else None,
                'legal_basis_full': legal_bases[0]['full'] if legal_bases else None,
                'category': legal_bases[0]['category'] if legal_bases else None,
                'sub_category': legal_bases[0]['sub_category'] if legal_bases else None,
                'issue_method': None,
                'is_split': False,
                'parent_issuance_id': None,
                'split_reason': None,
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            
            all_issuances.append(issuance_data)
    
    return {
        'announcement': announcement_data,
        'issuances': all_issuances
    }


def upload_to_bigquery(data_list):
    """BigQueryにデータをアップロード"""
    announcements = [d['announcement'] for d in data_list if d]
    if announcements:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.announcements"
        errors = client.insert_rows_json(table_ref, announcements)
        if errors:
            print(f"❌ announcements投入エラー: {errors}")
        else:
            print(f"✅ announcements投入成功: {len(announcements)}件")
    
    all_issuances = []
    for d in data_list:
        if d and d['issuances']:
            all_issuances.extend(d['issuances'])
    
    if all_issuances:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.bond_issuances"
        errors = client.insert_rows_json(table_ref, all_issuances)
        if errors:
            print(f"❌ bond_issuances投入エラー: {errors[:3]}")
        else:
            print(f"✅ bond_issuances投入成功: {len(all_issuances)}件")
    
    return len(all_issuances)


def main():
    """メイン処理"""
    print("=" * 60)
    print("データ投入スクリプト（20251025データセット・完全修正版）")
    print("=" * 60)
    print()
    
    if not HAS_PARSERS:
        print("⚠️  警告: parsersモジュールが見つかりません")
        return
    
    files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.txt')])
    print(f"処理対象ファイル数: {len(files)}件\n")
    
    data_list = []
    error_count = 0
    
    for i, file_name in enumerate(files, 1):
        file_path = os.path.join(DATA_DIR, file_name)
        print(f"[{i}/{len(files)}] {file_name}")
        
        try:
            result = process_announcement_file(file_path)
            if result:
                data_list.append(result)
                if result['issuances']:
                    print(f"  ✅ 発行件数: {len(result['issuances'])}件")
        except Exception as e:
            error_count += 1
            print(f"  ❌ エラー: {e}")
    
    print(f"\n=== 処理完了 ===")
    print(f"成功: {len(data_list)}件")
    print(f"エラー: {error_count}件")
    
    if data_list:
        print("\n=== BigQuery投入開始 ===")
        total_issuances = upload_to_bigquery(data_list)
        print(f"\n総発行件数: {total_issuances}件")
        
        # 発行根拠別集計
        print("\n=== 発行根拠別集計 ===")
        legal_basis_summary = {}
        for d in data_list:
            if d and d['issuances']:
                for issuance in d['issuances']:
                    category = issuance.get('category')
                    if category:
                        if category not in legal_basis_summary:
                            legal_basis_summary[category] = {'count': 0, 'amount': 0}
                        legal_basis_summary[category]['count'] += 1
                        amount = issuance.get('issue_amount', 0) or 0
                        legal_basis_summary[category]['amount'] += amount
        
        for category in sorted(legal_basis_summary.keys()):
            data = legal_basis_summary[category]
            amount_oku = data['amount'] / 100000000
            print(f"{category}: {data['count']}件, {amount_oku:,.0f}億円")


if __name__ == "__main__":
    main()