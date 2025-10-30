"""
Phase 4 テスト: 個人向け国債
添付の告示（20230615）でテスト
"""

import re
import json
from datetime import datetime
from pathlib import Path
import os
from google.cloud import bigquery

# 認証設定
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json'

PROJECT_ID = "jgb2023"
DATASET_ID = "20251027"


class RetailBondParser:
    """個人向け国債の告示を解析"""
    
    def can_parse(self, text: str) -> bool:
        """このパーサーで処理可能か判定"""
        if '個人向け利付国庫債券' in text or '個人向け国債' in text:
            return True
        return False
    
    def extract(self, text: str) -> dict:
        """告示から番号付きリストを抽出"""
        
        items = {}
        
        # 項目1: 名称及び記号
        match = re.search(r'１\s+名称及び記号\s+(.+?)(?=\n２)', text, re.DOTALL)
        if match:
            items[1] = {
                'title': '名称及び記号',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_item1(match.group(1))
            }
        
        # 項目2: 発行の根拠法律
        match = re.search(r'２\s+発行の根拠法律及びその条項\s+(.+?)(?=\n３)', text, re.DOTALL)
        if match:
            items[2] = {
                'title': '発行の根拠法律及びその条項',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_laws(match.group(1))
            }
        
        # 項目4: 発行額（個人向けは項目4）
        match = re.search(r'４\s+発行額\s+額面金額で([\d,]+)円', text, re.DOTALL)
        if match:
            total_amount = int(match.group(1).replace(',', ''))
            items[4] = {
                'title': '発行額',
                'value': f'額面金額で{match.group(1)}円',
                'sub_number': None,
                'structured_data': {'total_amount': total_amount}
            }
        
        # 項目7: 発行日（個人向けは項目7）
        match = re.search(r'７\s+発行日\s+(.+?)(?=\n８)', text, re.DOTALL)
        if match:
            items[7] = {
                'title': '発行日',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_date(match.group(1))
            }
        
        # 項目9: 初期利子の適用利率
        match = re.search(r'９\s+初期利子の適用利率\s+年([\d.]+)％', text, re.DOTALL)
        if match:
            items[9] = {
                'title': '初期利子の適用利率',
                'value': f'年{match.group(1)}％',
                'sub_number': None,
                'structured_data': {'rate': float(match.group(1))}
            }
        
        # 項目13: 償還期限
        match = re.search(r'13\s+償還期限\s+(.+?)(?=\n14)', text, re.DOTALL)
        if match:
            items[13] = {
                'title': '償還期限',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_date(match.group(1))
            }
        
        # 項目14: 償還金額
        match = re.search(r'14\s+償還金額\s+(.+?)(?=\n15)', text, re.DOTALL)
        if match:
            items[14] = {
                'title': '償還金額',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_redemption_amount(match.group(1))
            }
        
        return items
    
    def _parse_item1(self, text: str) -> dict:
        """項目1: 名称及び記号を構造化"""
        # 例: "個人向け利付国庫債券（変動・10年）（第158回）"
        match = re.search(r'個人向け利付国庫債券[（\(](.+?)[）\)][（\(]第(\d+)回[）\)]', text)
        if match:
            return {
                'bond_name': f'個人向け利付国庫債券（{match.group(1)}）',
                'bond_series': f'第{match.group(2)}回',
                'bond_type': match.group(1)  # "変動・10年"
            }
        return {'raw': text}
    
    def _parse_laws(self, text: str) -> dict:
        """法令を抽出"""
        laws = []
        
        text_normalized = text.replace('４', '4').replace('６', '6').replace('１', '1')
        
        print(f"    [DEBUG] 項目2の元テキスト: {text[:100]}...")
        
        # 特別会計法第46条
        if re.search(r'特別会計.*?第46条第1項', text_normalized, re.DOTALL):
            print("    [DEBUG] ✅ 特別会計法第46条を検出")
            laws.append({
                'name': '特別会計に関する法律',
                'full_name': '特別会計に関する法律（平成19年法律第23号）',
                'article': '第46条第1項',
                'key': '特別会計に関する法律第46条第1項',
                'amount': 0
            })
        else:
            print("    [DEBUG] ❌ 特別会計法第46条を検出できず")
        
        print(f"    [DEBUG] 最終的に抽出された法令数: {len(laws)}")
        
        return {'laws': laws}
    
    def _parse_date(self, text: str) -> dict:
        """発行日を解析"""
        match = re.search(r'令和(\d+)年(\d+)月(\d+)日', text)
        if match:
            year = int(match.group(1)) + 2018
            month = int(match.group(2))
            day = int(match.group(3))
            return {'issue_date': f'{year}-{month:02d}-{day:02d}'}
        return {'raw': text}
    
    def _parse_redemption_amount(self, text: str) -> dict:
        """償還金額を解析"""
        match = re.search(r'額面金額100円につき(\d+)円', text)
        if match:
            return {'amount': int(match.group(1))}
        return {'raw': text}


def insert_to_bigquery(announcement_id: str, raw_text: str, items: dict, file_path: Path):
    """BigQueryにデータを投入"""
    
    client = bigquery.Client(project=PROJECT_ID)
    
    issue_date = items.get(7, {}).get('structured_data', {}).get('issue_date', '2023-06-15')
    announcement_date = '2023-07-11'
    announcement_number = '第百九十二号'
    
    # Layer 1: raw_announcements
    raw_row = {
        'announcement_id': announcement_id,
        'announcement_date': announcement_date,
        'announcement_number': announcement_number,
        'issue_date': issue_date,
        'format_pattern': 'RETAIL_BOND',
        'format_pattern_confidence': 1.0,
        'raw_text': raw_text,
        'source_file': file_path.name,
        'file_path': str(file_path),
        'parsed': True,
        'parse_error': None,
        'parsed_at': datetime.now().isoformat()
    }
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_announcements"
    errors = client.insert_rows_json(table_ref, [raw_row])
    
    if errors:
        print(f"❌ Layer 1 投入エラー: {errors}")
        return False
    else:
        print(f"✅ Layer 1 投入成功: {announcement_id}")
    
    # Layer 2: announcement_items
    item_rows = []
    for key, item_data in items.items():
        if isinstance(key, str) and '_' in key:
            parts = key.split('_')
            item_number = int(parts[0])
            sub_number = int(parts[1])
        else:
            item_number = int(key)
            sub_number = item_data.get('sub_number')
        
        item_row = {
            'item_id': f"{announcement_id}_item{key}",
            'announcement_id': announcement_id,
            'item_number': item_number,
            'sub_number': sub_number,
            'item_title': item_data['title'],
            'item_value': item_data['value'],
            'structured_data': json.dumps(item_data['structured_data'], ensure_ascii=False)
        }
        item_rows.append(item_row)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.announcement_items"
    errors = client.insert_rows_json(table_ref, item_rows)
    
    if errors:
        print(f"❌ Layer 2 投入エラー: {errors}")
        return False
    else:
        print(f"✅ Layer 2 投入成功: {len(item_rows)} 項目")
    
    return True


def test_retail_bond():
    """個人向け国債のテスト"""
    
    file_path = Path(r"G:\マイドライブ\JGBデータ\2023\20230615_令和5年7月11日付（財務省第百九十二号）.txt")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
    
    print("=" * 70)
    print("Phase 4 テスト: 個人向け国債")
    print("=" * 70)
    print(f"📄 ファイル: {file_path.name}")
    print(f"📝 文字数: {len(text):,}")
    print()
    
    # パーサー判定
    parser = RetailBondParser()
    if not parser.can_parse(text):
        print("❌ このファイルは個人向け国債ではありません")
        return
    
    print("✅ 個人向け国債を検出")
    print()
    
    # パース実行
    items = parser.extract(text)
    
    print(f"✅ パース完了: {len(items)} 項目抽出")
    print()
    
    # 結果サマリー
    print("-" * 70)
    print("📊 抽出結果サマリー")
    print("-" * 70)
    
    if 1 in items:
        bond_info = items[1]['structured_data']
        print(f"銘柄: {bond_info.get('bond_name', 'N/A')} {bond_info.get('bond_series', 'N/A')}")
        print(f"タイプ: {bond_info.get('bond_type', 'N/A')}")
    
    if 4 in items:
        total_amount = items[4]['structured_data'].get('total_amount', 0)
        print(f"発行額: {total_amount:,}円（{total_amount/1_000_000_000_000:.2f}兆円）")
    
    if 7 in items:
        print(f"発行日: {items[7]['structured_data'].get('issue_date', 'N/A')}")
    
    if 9 in items:
        print(f"初期利率: {items[9]['structured_data'].get('rate', 'N/A')}%")
    
    if 13 in items:
        maturity_date = items[13]['structured_data'].get('issue_date', 'N/A')
        print(f"償還期限: {maturity_date}")
    
    print()
    
    # 根拠法令の表示
    if 2 in items:
        print("-" * 70)
        print("⚖️ 発行の根拠法律")
        print("-" * 70)
        
        laws = items[2]['structured_data'].get('laws', [])
        
        if laws:
            for law in laws:
                print(f"  {law['key']}")
            
            # 発行額を法令に割り当て
            if 4 in items:
                total_amount = items[4]['structured_data'].get('total_amount', 0)
                print()
                print(f"{'法令':<55} {'発行額（円）':>20} {'兆円':>10}")
                print("-" * 90)
                print(f"{laws[0]['key']:<55} {total_amount:>20,} {total_amount/1_000_000_000_000:>9.2f}")
        else:
            print("  法令が抽出できませんでした")
        
        print()
    
    # BigQueryへ投入
    print("-" * 70)
    print("📤 BigQuery へデータ投入")
    print("-" * 70)
    
    announcement_id = "20230615_192"
    success = insert_to_bigquery(announcement_id, text, items, file_path)
    
    if success:
        print()
        print("=" * 70)
        print("✅ テスト完了！")
        print("=" * 70)
        print(f"Announcement ID: {announcement_id}")
        print(f"Layer 1: raw_announcements に 1行 投入")
        print(f"Layer 2: announcement_items に {len(items)}行 投入")
        print()
        print("次のステップ: BigQueryコンソールでデータを確認してください")
    else:
        print()
        print("❌ データ投入に失敗しました")
    
    return items


if __name__ == "__main__":
    test_retail_bond()