"""
Phase 4 テスト: 横並び別表形式
添付の告示（20230414）でテスト
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


class TableParserV4:
    """横並び別表形式の告示を解析"""
    
    def can_parse(self, text: str) -> bool:
        """このパーサーで処理可能か判定"""
        if '（別表のとおり）' in text or '内訳（別表のとおり）' in text:
            if '名称及び記号' in text and '利率' in text and '償還期限' in text:
                return True
        return False
    
    def extract(self, text: str) -> dict:
        """告示から番号付きリストと別表を抽出"""
        
        items = {}
        
        # 項目1: 名称及び記号（複数銘柄）
        match = re.search(r'１\s+名称及び記号\s+(.+?)(?=\n２)', text, re.DOTALL)
        if match:
            bond_names = self._parse_multiple_bond_names(match.group(1))
            items[1] = {
                'title': '名称及び記号',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': {'bond_names': bond_names}
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
        
        # 項目6: 発行額（別表参照）
        match = re.search(r'６\s+発行額\s+額面金額で([\d,]+)円', text, re.DOTALL)
        if match:
            total_amount = int(match.group(1).replace(',', ''))
            items[6] = {
                'title': '発行額',
                'value': f'額面金額で{match.group(1)}円\n内訳（別表のとおり）',
                'sub_number': None,
                'structured_data': {'total_amount': total_amount}
            }
        
        # 項目10: 発行日
        match = re.search(r'10\s+発行日\s+(.+?)(?=\n11)', text, re.DOTALL)
        if match:
            items[10] = {
                'title': '発行日',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_date(match.group(1))
            }
        
        # 別表の解析
        table_data = self._parse_table(text)
        if table_data:
            print(f"    [INFO] 別表から {len(table_data)} 行を抽出")
            # 別表の各行を項目6のサブ項目として追加
            for i, row in enumerate(table_data, start=1):
                items[f'6_{i}'] = {
                    'title': '発行額（別表）',
                    'value': self._format_table_row(row),
                    'sub_number': i,
                    'structured_data': row
                }
        
        return items
    
    def _parse_multiple_bond_names(self, text: str) -> list:
        """複数の銘柄名を抽出"""
        bond_names = []
        parts = re.split(r'及び', text)
        
        for part in parts:
            part = part.strip()
            if '利付国庫債券' in part:
                bond_names.append(part)
        
        return bond_names
    
    def _parse_laws(self, text: str) -> dict:
        """法令を抽出"""
        laws = []
        
        text_normalized = text.replace('４', '4').replace('６', '6').replace('２', '2').replace('１', '1')
        
        if re.search(r'特別会計.*?第46条第1項', text_normalized, re.DOTALL):
            laws.append({
                'name': '特別会計に関する法律',
                'full_name': '特別会計に関する法律（平成19年法律第23号）',
                'article': '第46条第1項',
                'key': '特別会計に関する法律第46条第1項',
                'amount': 0
            })
        
        if re.search(r'特別会計.*?第62条第1項', text_normalized, re.DOTALL):
            laws.append({
                'name': '特別会計に関する法律',
                'full_name': '特別会計に関する法律（平成19年法律第23号）',
                'article': '第62条第1項',
                'key': '特別会計に関する法律第62条第1項',
                'amount': 0
            })
        
        print(f"    [DEBUG] 法令を {len(laws)} 件抽出")
        for law in laws:
            print(f"    [DEBUG]   - {law['key']}")
        
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
    
    def _parse_table(self, text: str) -> list:
        """別表を解析"""
        
        table_start = text.find('（別表）')
        if table_start == -1:
            return []
        
        table_text = text[table_start:]
        
        rows = []
        
        pattern = r'利付国庫債券（(.+?)）（第(\d+)回）\s+([\d.]+)％\s+令和(\d+)年(\d+)月(\d+)日\s+特別会計に関する法律第(\d+)条第１項分\s+([\d,]+)円'
        
        matches = re.finditer(pattern, table_text)
        
        for match in matches:
            bond_type = match.group(1)
            series = match.group(2)
            rate = float(match.group(3))
            maturity_year = int(match.group(4)) + 2018
            maturity_month = int(match.group(5))
            maturity_day = int(match.group(6))
            law_article = match.group(7)
            amount = int(match.group(8).replace(',', ''))
            
            law_key = f'特別会計に関する法律第{law_article}条第1項'
            
            row = {
                'bond_name': f'利付国庫債券（{bond_type}）',
                'bond_series': f'第{series}回',
                'interest_rate': rate,
                'maturity_date': f'{maturity_year}-{maturity_month:02d}-{maturity_day:02d}',
                'law_key': law_key,
                'law_article': f'第{law_article}条第1項',
                'issue_amount': amount
            }
            
            rows.append(row)
        
        return rows
    
    def _format_table_row(self, row: dict) -> str:
        """表の行を文字列にフォーマット"""
        return (f"{row['bond_name']}{row['bond_series']}: "
                f"利率{row['interest_rate']}%, "
                f"償還期限{row['maturity_date']}, "
                f"{row['law_key']}, "
                f"発行額{row['issue_amount']:,}円")


def insert_to_bigquery(announcement_id: str, raw_text: str, items: dict, file_path: Path):
    """BigQueryにデータを投入"""
    
    client = bigquery.Client(project=PROJECT_ID)
    
    issue_date = items.get(10, {}).get('structured_data', {}).get('issue_date', '2023-04-14')
    announcement_date = '2023-05-09'
    announcement_number = '第百二十七号'
    
    # Layer 1: raw_announcements
    raw_row = {
        'announcement_id': announcement_id,
        'announcement_date': announcement_date,
        'announcement_number': announcement_number,
        'issue_date': issue_date,
        'format_pattern': 'TABLE_HORIZONTAL',
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


def test_table_parser():
    """横並び別表形式のテスト"""
    
    file_path = Path(r"G:\マイドライブ\JGBデータ\2023\20230414_令和5年5月9日付（財務省第百二十七号）.txt")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
    
    print("=" * 70)
    print("Phase 4 テスト: 横並び別表形式")
    print("=" * 70)
    print(f"📄 ファイル: {file_path.name}")
    print(f"📝 文字数: {len(text):,}")
    print()
    
    # パーサー判定
    parser = TableParserV4()
    if not parser.can_parse(text):
        print("❌ このファイルは横並び別表形式ではありません")
        return
    
    print("✅ 横並び別表形式を検出")
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
        bond_names = items[1]['structured_data'].get('bond_names', [])
        print(f"銘柄数: {len(bond_names)}")
        print(f"例: {bond_names[0] if bond_names else 'N/A'}")
    
    if 6 in items:
        total_amount = items[6]['structured_data'].get('total_amount', 0)
        print(f"総発行額: {total_amount:,}円（{total_amount/1_000_000_000_000:.2f}兆円）")
    
    if 10 in items:
        print(f"発行日: {items[10]['structured_data'].get('issue_date', 'N/A')}")
    
    # 別表の行数をカウント
    table_rows = [k for k in items.keys() if isinstance(k, str) and k.startswith('6_')]
    print(f"別表の行数: {len(table_rows)}")
    
    print()
    
    # 根拠法令別の発行額を集計
    if 2 in items and len(table_rows) > 0:
        print("-" * 70)
        print("💰 根拠法令別の発行額集計（別表から）")
        print("-" * 70)
        
        law_totals = {}
        
        for key in table_rows:
            row_data = items[key]['structured_data']
            law_key = row_data.get('law_key')
            amount = row_data.get('issue_amount', 0)
            
            if law_key:
                law_totals[law_key] = law_totals.get(law_key, 0) + amount
        
        print(f"\n{'法令':<55} {'発行額（円）':>20} {'兆円':>10}")
        print("-" * 90)
        
        total = 0
        for law_key, amount in sorted(law_totals.items(), key=lambda x: x[1], reverse=True):
            trillion = amount / 1_000_000_000_000
            print(f"{law_key:<55} {amount:>20,} {trillion:>9.2f}")
            total += amount
        
        print("-" * 90)
        print(f"{'合計':<55} {total:>20,} {total/1_000_000_000_000:>9.2f}")
        print()
    
    # BigQueryへ投入
    print("-" * 70)
    print("📤 BigQuery へデータ投入")
    print("-" * 70)
    
    announcement_id = "20230414_127"
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
    test_table_parser()