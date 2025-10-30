"""
データ投入スクリプト（20251024データセット用）

改善版：発行根拠法令の抽出と分離に対応（2023年度版）
階層構造：category（大分類）、sub_category（詳細分類）

政府短期証券が含まれる告示でも、割引短期国債は抽出する
"""

import os
import sys
import re
import json
from datetime import datetime, date, timedelta
import uuid

# プロジェクトルートをパスに追加
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from google.cloud import bigquery

# パーサーのインポートを試みる
try:
    from parsers.table_parser import TableParser
    from parsers.issue_extractor import IssueExtractor
    HAS_PARSERS = True
except ImportError:
    print("⚠️  警告: parsersモジュールが見つかりません。簡易モードで実行します。")
    HAS_PARSERS = False

# BigQuery設定
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json"
PROJECT_ID = 'jgb2023'
DATASET_ID = '20251024'

client = bigquery.Client(project=PROJECT_ID)

# データディレクトリ
DATA_DIR = r"G:\マイドライブ\JGBデータ\2023"

# 発行根拠法令パターン（2023年度用・階層構造版）
LEGAL_BASIS_PATTERNS = {
    '4条債': {
        'category': '4条債',
        'sub_category': '建設国債',
        'patterns': [
            r'財政法.*?第4条第1項',
            r'財政法.*?第四条第一項'
        ]
    },
    '特例債': {
        'category': '特例債',
        'sub_category': '特例公債',
        'patterns': [
            r'財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律.*?第2条第1項',
            r'財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律.*?第二条第一項',
            r'平成24年法律第101号.*?第2条第1項'
        ]
    },
    'GX経済移行債': {
        'category': 'GX経済移行債',
        'sub_category': 'つなぎ公債',
        'patterns': [
            r'脱炭素成長型経済構造への円滑な移行の推進に関する法律.*?第7条第1項',
            r'脱炭素成長型経済構造への円滑な移行の推進に関する法律.*?第七条第一項',
            r'令和5年法律第32号.*?第7条第1項'
        ]
    },
    '復興債': {
        'category': '復興債',
        'sub_category': 'つなぎ公債',
        'patterns': [
            r'東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法.*?第69条第4項',
            r'東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法.*?第六十九条第四項'
        ]
    },
    '年金特例債': {
        'category': '年金特例債',
        'sub_category': 'つなぎ公債',
        'patterns': [
            r'財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律.*?第3条第1項',
            r'財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律.*?第三条第一項',
            r'平成24年法律第101号.*?第3条第1項'
        ]
    },
    '借換債': {
        'category': '借換債',
        'sub_category': '借換債',
        'patterns': [
            r'特別会計に関する法律.*?第46条第1項',
            r'特別会計に関する法律.*?第四十六条第一項'
        ]
    },
    '前倒債': {
        'category': '借換債',
        'sub_category': '前倒債',
        'patterns': [
            r'特別会計に関する法律.*?第47条第1項',
            r'特別会計に関する法律.*?第四十七条第一項'
        ]
    },
    '財投債': {
        'category': '財投債',
        'sub_category': '財投債',
        'patterns': [
            r'特別会計に関する法律.*?第62条第1項',
            r'特別会計に関する法律.*?第六十二条第一項'
        ]
    }
}

# 政府短期証券のパターン（除外用）
SEIFU_TANKI_PATTERNS = [
    r'財政法.*?第7条第1項',
    r'財政融資資金法.*?第9条第1項',
    r'特別会計に関する法律.*?第83条',
    r'特別会計に関する法律.*?第94条',
    r'特別会計に関する法律.*?第95条',
    r'特別会計に関する法律.*?第136条',
    r'特別会計に関する法律.*?第137条'
]


def parse_date_string(date_value):
    """日付文字列またはdateオブジェクトをdateオブジェクトに変換"""
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
    """発行根拠法令を抽出（複数対応、金額も抽出）"""
    results = []
    
    for basis_name, config in LEGAL_BASIS_PATTERNS.items():
        for pattern in config['patterns']:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                full_text = match.group(0)
                amount = extract_amount_for_legal_basis(text, full_text)
                
                results.append({
                    'basis': basis_name,
                    'category': config['category'],
                    'sub_category': config['sub_category'],
                    'full': full_text,
                    'amount': amount
                })
                break
    
    return results


def extract_amount_for_legal_basis(text, legal_text):
    """特定の発行根拠に紐づく金額を抽出"""
    pattern1 = re.escape(legal_text) + r'[^円]*?額面金額で([\d,]+)円'
    match = re.search(pattern1, text, re.IGNORECASE | re.DOTALL)
    
    if match:
        return int(match.group(1).replace(',', ''))
    
    pattern2 = re.escape(legal_text) + r'[^円]*?金額([\d,]+)円'
    match = re.search(pattern2, text, re.IGNORECASE | re.DOTALL)
    
    if match:
        return int(match.group(1).replace(',', ''))
    
    return None


def has_waribiki_tanki(text):
    """
    割引短期国債が含まれているか判定
    
    「割引短期国債」という明示的な記述を探す
    """
    return '割引短期国債' in text


def has_government_short_term_bond(text):
    """政府短期証券が含まれているか判定"""
    has_seifu_pattern = any(re.search(p, text, re.IGNORECASE) for p in SEIFU_TANKI_PATTERNS)
    has_seifu_word = '政府短期証券' in text
    return has_seifu_pattern or has_seifu_word


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
    if '短期' in bond_name or 'TANKI' in bond_name:
        if maturity_days and maturity_days >= 270:
            return 'BOND_TANKI_1Y'
        else:
            return 'BOND_TANKI_6M'
    
    if 'クライメート' in bond_name or 'トランジション' in bond_name:
        if '10年' in bond_name:
            return 'BOND_GX_10Y'
        elif '5年' in bond_name:
            return 'BOND_GX_5Y'
    
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
    
    for key, value in mapping.items():
        if key in bond_name:
            return value
    
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


def split_by_legal_basis(issuance, legal_bases, announcement_text):
    """1つの発行を複数の発行根拠に分離"""
    if len(legal_bases) == 0:
        return [issuance]
    
    if len(legal_bases) == 1:
        issuance['legal_basis'] = legal_bases[0]['basis']
        issuance['legal_basis_full'] = legal_bases[0]['full']
        issuance['legal_basis_category'] = legal_bases[0]['category']
        return [issuance]
    
    split_issuances = []
    total_extracted_amount = sum([lb['amount'] for lb in legal_bases if lb['amount']])
    original_amount = issuance['issue_amount']
    
    if total_extracted_amount > 0 and original_amount and abs(total_extracted_amount - original_amount) > original_amount * 0.01:
        print(f"  ⚠️  金額不一致: 合計{total_extracted_amount:,}円 vs 元{original_amount:,}円")
    
    for lb in legal_bases:
        new_issuance = issuance.copy()
        new_issuance['issuance_id'] = str(uuid.uuid4())
        new_issuance['legal_basis'] = lb['basis']
        new_issuance['legal_basis_full'] = lb['full']
        new_issuance['legal_basis_category'] = lb['category']
        new_issuance['is_split'] = True
        new_issuance['parent_issuance_id'] = issuance['issuance_id']
        new_issuance['split_reason'] = '複数発行根拠'
        
        if lb['amount']:
            new_issuance['issue_amount'] = lb['amount']
        else:
            unassigned_count = sum(1 for x in legal_bases if not x['amount'])
            if unassigned_count > 0 and original_amount:
                remaining = original_amount - total_extracted_amount
                new_issuance['issue_amount'] = remaining // unassigned_count
        
        split_issuances.append(new_issuance)
    
    return split_issuances


def process_announcement_file(file_path):
    """1つの告示ファイルを処理"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    file_name = os.path.basename(file_path)
    announcement_id = file_name.replace('.txt', '')
    
    # 割引短期国債と政府短期証券の判定
    has_waribiki = has_waribiki_tanki(content)
    has_seifu = has_government_short_term_bond(content)
    
    # 政府短期証券のみ（割引短期国債なし）の場合はスキップ
    if has_seifu and not has_waribiki:
        print(f"  ⚠️  政府短期証券のみ（スキップ）")
        return None
    
    # 両方含まれている場合は警告
    if has_waribiki and has_seifu:
        print(f"  📝 割引短期国債+政府短期証券（割引短期国債のみ抽出）")
    
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
    
    legal_bases = extract_legal_bases(content)
    
    if legal_bases:
        # 政府短期証券関連の発行根拠は除外
        legal_bases_filtered = []
        for lb in legal_bases:
            # 借換債（特会法46条）のみを残す（割引短期国債の場合）
            if lb['basis'] in ['借換債', '前倒債', '4条債', '特例債', 'GX経済移行債', '復興債']:
                legal_bases_filtered.append(lb)
        
        if legal_bases_filtered:
            basis_summary = ', '.join([f"{lb['basis']}({lb['category']})" for lb in legal_bases_filtered])
            print(f"  → 発行根拠: {basis_summary}")
            legal_bases = legal_bases_filtered
    
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
                'legal_basis': None,
                'legal_basis_full': None,
                'legal_basis_category': None,
                'issue_method': None,
                'is_split': False,
                'parent_issuance_id': None,
                'split_reason': None,
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            
            split_issuances = split_by_legal_basis(issuance_data, legal_bases, content)
            all_issuances.extend(split_issuances)
    else:
        print("  ⚠️  パーサー未使用（発行根拠のみ記録）")
    
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
            print(f"❌ bond_issuances投入エラー: {errors}")
        else:
            print(f"✅ bond_issuances投入成功: {len(all_issuances)}件")
    
    return len(all_issuances)


def main():
    """メイン処理"""
    print("=" * 60)
    print("データ投入スクリプト（20251024データセット）")
    print("=" * 60)
    print()
    
    if not HAS_PARSERS:
        print("⚠️  警告: parsersモジュールが見つかりません")
        print("⚠️  プロジェクトルート:", project_root)
        print("⚠️  発行根拠法令のみを処理します\n")
    
    files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.txt')])
    print(f"処理対象ファイル数: {len(files)}件\n")
    
    data_list = []
    skip_count = 0
    mixed_count = 0
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
                    # 混在告示のカウント
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        if has_waribiki_tanki(content) and has_government_short_term_bond(content):
                            mixed_count += 1
            else:
                skip_count += 1
        except Exception as e:
            error_count += 1
            print(f"  ❌ エラー: {e}")
    
    print(f"\n=== 処理完了 ===")
    print(f"成功: {len(data_list)}件")
    print(f"  うち混在告示: {mixed_count}件（割引短期国債+政府短期証券）")
    print(f"スキップ: {skip_count}件（政府短期証券のみ）")
    print(f"エラー: {error_count}件")
    
    if data_list:
        print("\n=== BigQuery投入開始 ===")
        total_issuances = upload_to_bigquery(data_list)
        print(f"\n総発行件数: {total_issuances}件")
        
        print("\n=== 発行根拠別集計 ===")
        legal_basis_summary = {}
        for d in data_list:
            if d and d['issuances']:
                for issuance in d['issuances']:
                    category = issuance.get('legal_basis_category')
                    if category:
                        if category not in legal_basis_summary:
                            legal_basis_summary[category] = {'count': 0, 'amount': 0}
                        legal_basis_summary[category]['count'] += 1
                        legal_basis_summary[category]['amount'] += issuance.get('issue_amount', 0) or 0
        
        for category in sorted(legal_basis_summary.keys()):
            data = legal_basis_summary[category]
            print(f"{category}: {data['count']}件, {data['amount'] / 100000000:,.0f}億円")


if __name__ == "__main__":
    main()