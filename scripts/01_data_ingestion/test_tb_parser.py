"""
Phase 4 テスト: 国庫短期証券
添付の告示（20231211）でテスト
"""

import re
import json
from datetime import datetime
from pathlib import Path
import os
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json'

PROJECT_ID = "jgb2023"
DATASET_ID = "20251027"


class TBParser:
    """国庫短期証券（Treasury Bill）の告示を解析"""
    
    def can_parse(self, text: str) -> bool:
        """このパーサーで処理可能か判定"""
        if '国庫短期証券' in text or '割引短期国債' in text or '政府短期証券' in text:
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
        
        # 項目2: 発行の根拠法律（複雑）
        match = re.search(r'２\s+発行の根拠法律及びその条項\s+(.+?)(?=\n３)', text, re.DOTALL)
        if match:
            items[2] = {
                'title': '発行の根拠法律及びその条項',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_laws(match.group(1))
            }
        
        # 項目6: 発行額（複雑：2種類の証券）
        match = re.search(r'６\s+発行額(.+?)(?=\n７)', text, re.DOTALL)
        if match:
            full_text = match.group(1).strip()
            items[6] = {
                'title': '発行額',
                'value': full_text,
                'sub_number': None,
                'structured_data': self._parse_item6(full_text)
            }
            
            # サブ項目も抽出（⑴⑵）
            sub_items = self._extract_sub_items(full_text)
            for sub_num, sub_data in sub_items.items():
                items[f'6_{sub_num}'] = {
                    'title': '発行額⑴⑵',
                    'value': sub_data['value'],
                    'sub_number': sub_num,
                    'structured_data': sub_data['structured']
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
        
        # 項目12: 償還期限
        match = re.search(r'12\s+償還期限\s+(.+?)(?=\n13)', text, re.DOTALL)
        if match:
            items[12] = {
                'title': '償還期限',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_date(match.group(1))
            }
        
        # 項目13: 償還金額
        match = re.search(r'13\s+償還金額\s+(.+?)(?=\n14)', text, re.DOTALL)
        if match:
            items[13] = {
                'title': '償還金額',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_redemption_amount(match.group(1))
            }
        
        return items
    
    def _parse_item1(self, text: str) -> dict:
        """項目1: 名称及び記号を構造化"""
        # 例: "国庫短期証券（第1199回）"
        match = re.search(r'国庫短期証券[（\(]第(\d+)回[）\)]', text)
        if match:
            return {
                'bond_name': '国庫短期証券',
                'bond_series': f'第{match.group(1)}回'
            }
        return {'raw': text}
    
    def _parse_laws(self, text: str) -> dict:
        """法令を抽出（複雑：複数法令）"""
        laws = []
        
        text_normalized = text.replace('４', '4').replace('６', '6').replace('７', '7').replace('９', '9').replace('３', '3').replace('８', '8').replace('１', '1').replace('２', '2').replace('５', '5')
        
        print(f"    [DEBUG] 項目2の元テキスト（最初の100文字）: {text[:100]}...")
        
        # 特別会計法第46条第1項
        if re.search(r'特別会計.*?第46条第1項', text_normalized, re.DOTALL):
            print("    [DEBUG] ✅ 特別会計法第46条を検出")
            laws.append({
                'name': '特別会計に関する法律',
                'full_name': '特別会計に関する法律（平成19年法律第23号）',
                'article': '第46条第1項',
                'key': '特別会計に関する法律第46条第1項',
                'amount': 0
            })
        
        # 財政法第7条第1項
        if re.search(r'財政法.*?第7条第1項', text_normalized, re.DOTALL):
            print("    [DEBUG] ✅ 財政法第7条を検出")
            laws.append({
                'name': '財政法',
                'full_name': '財政法（昭和22年法律第34号）',
                'article': '第7条第1項',
                'key': '財政法第7条第1項',
                'amount': 0
            })
        
        # 財政融資資金法第9条第1項
        if re.search(r'財政融資資金法.*?第9条第1項', text_normalized, re.DOTALL):
            print("    [DEBUG] ✅ 財政融資資金法第9条を検出")
            laws.append({
                'name': '財政融資資金法',
                'full_name': '財政融資資金法（昭和26年法律第100号）',
                'article': '第9条第1項',
                'key': '財政融資資金法第9条第1項',
                'amount': 0
            })
        
        # 特別会計法の複数条項（政府短期証券用）
        tb_articles = ['83', '94', '95', '136', '137']
        for article in tb_articles:
            if f'第{article}条第1項' in text_normalized or f'第{article}条第2項' in text_normalized or f'第{article}条第4項' in text_normalized:
                print(f"    [DEBUG] ✅ 特別会計法第{article}条を検出")
                # 簡略化のため、まとめて1つのエントリーとする
        
        # 政府短期証券用の法令グループ
        if any(f'第{a}条' in text_normalized for a in tb_articles):
            laws.append({
                'name': '特別会計に関する法律（政府短期証券）',
                'full_name': '特別会計に関する法律（複数条項）',
                'article': '第83条等',
                'key': '政府短期証券関連法令',
                'amount': 0
            })
        
        print(f"    [DEBUG] 最終的に抽出された法令数: {len(laws)}")
        
        return {'laws': laws}
    
    def _extract_sub_items(self, text: str) -> dict:
        """⑴⑵のサブ項目を抽出"""
        sub_items = {}
        
        # ⑴ 価格競争入札発行
        match1 = re.search(r'⑴\s*価格競争入札発行\s*額面金額で([\d,]+)円(.+?)(?=⑵|$)', text, re.DOTALL)
        if match1:
            total_amount = int(match1.group(1).replace(',', ''))
            detail = match1.group(2)
            
            sub_items[1] = {
                'value': f'⑴ 価格競争入札発行 額面金額で{match1.group(1)}円{detail[:100]}...',
                'structured': self._parse_sub_item_amount(detail, total_amount)
            }
        
        # ⑵ 第Ⅰ非価格競争入札発行
        match2 = re.search(r'⑵\s*国債市場特別参加者・第Ⅰ非価格競争入札発行(.+?)$', text, re.DOTALL)
        if match2:
            detail = match2.group(1)
            
            # 発行額を抽出
            amount_match = re.search(r'額面金額で([\d,]+)円', detail)
            if amount_match:
                total_amount = int(amount_match.group(1).replace(',', ''))
                
                sub_items[2] = {
                    'value': f'⑵ 国債市場特別参加者・第Ⅰ非価格競争入札発行{detail[:100]}...',
                    'structured': self._parse_sub_item_amount(detail, total_amount)
                }
        
        return sub_items
    
    def _parse_sub_item_amount(self, text: str, total_amount: int) -> dict:
        """サブ項目から発行額を抽出"""
        result = {'total_amount': total_amount, 'by_law': {}}
        
        # 割引短期国債（特別会計法第46条第1項）
        tb_match = re.search(r'特別会計に関する法律第46条第１項の規定に基づき発行した割引短期国債.*?額面金額で([\d,]+)円', text, re.DOTALL)
        if tb_match:
            amount = int(tb_match.group(1).replace(',', ''))
            result['by_law']['特別会計に関する法律第46条第1項'] = amount
            print(f"    [INFO] 割引短期国債: {amount:,}円")
        
        # 政府短期証券（複数法令）
        gsb_match = re.search(r'財政法第７条第１項.*?政府短期証券.*?額面金額で([\d,]+)円', text, re.DOTALL)
        if gsb_match:
            amount = int(gsb_match.group(1).replace(',', ''))
            result['by_law']['政府短期証券関連法令'] = amount
            print(f"    [INFO] 政府短期証券: {amount:,}円")
        
        return result
    
    def _parse_item6(self, text: str) -> dict:
        """項目6: 発行額を構造化"""
        result = {
            'total_amount': 0,
            'competitive': {'amount': 0, 'by_law': {}},
            'noncompetitive1': {'amount': 0, 'by_law': {}}
        }
        
        # ⑴ 価格競争入札
        comp_match = re.search(r'⑴\s*価格競争入札発行\s*額面金額で([\d,]+)円', text)
        if comp_match:
            amount = int(comp_match.group(1).replace(',', ''))
            result['competitive']['amount'] = amount
            result['total_amount'] += amount
            
            comp_section = re.search(r'⑴(.+?)(?=⑵|$)', text, re.DOTALL)
            if comp_section:
                result['competitive']['by_law'] = self._extract_law_amounts_from_section(comp_section.group(1))
        
        # ⑵ 第Ⅰ非価格競争
        nc1_match = re.search(r'⑵.*?額面金額で([\d,]+)円', text, re.DOTALL)
        if nc1_match:
            amount = int(nc1_match.group(1).replace(',', ''))
            result['noncompetitive1']['amount'] = amount
            result['total_amount'] += amount
            
            nc1_section = re.search(r'⑵(.+?)$', text, re.DOTALL)
            if nc1_section:
                result['noncompetitive1']['by_law'] = self._extract_law_amounts_from_section(nc1_section.group(1))
        
        return result
    
    def _extract_law_amounts_from_section(self, section: str) -> dict:
        """セクションから法令別の発行額を抽出"""
        by_law = {}
        
        # 割引短期国債
        tb_match = re.search(r'特別会計に関する法律第46条第１項の規定に基づき発行した割引短期国債.*?額面金額で([\d,]+)円', section, re.DOTALL)
        if tb_match:
            amount = int(tb_match.group(1).replace(',', ''))
            by_law['特別会計に関する法律第46条第1項'] = amount
        
        # 政府短期証券
        gsb_match = re.search(r'財政法第７条第１項.*?政府短期証券.*?額面金額で([\d,]+)円', section, re.DOTALL)
        if gsb_match:
            amount = int(gsb_match.group(1).replace(',', ''))
            by_law['政府短期証券関連法令'] = amount
        
        return by_law
    
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
    
    issue_date = items.get(10, {}).get('structured_data', {}).get('issue_date', '2023-12-11')
    announcement_date = '2024-01-12'
    announcement_number = '第十六号'
    
    raw_row = {
        'announcement_id': announcement_id,
        'announcement_date': announcement_date,
        'announcement_number': announcement_number,
        'issue_date': issue_date,
        'format_pattern': 'TB_SHORT_TERM',
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


def test_tb():
    """国庫短期証券のテスト"""
    
    file_path = Path(r"G:\マイドライブ\JGBデータ\2023\20231211_令和6年1月12日付（財務省第十六号）.txt")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
    
    print("=" * 70)
    print("Phase 4 テスト: 国庫短期証券")
    print("=" * 70)
    print(f"📄 ファイル: {file_path.name}")
    print(f"📝 文字数: {len(text):,}")
    print()
    
    parser = TBParser()
    if not parser.can_parse(text):
        print("❌ このファイルは国庫短期証券ではありません")
        return
    
    print("✅ 国庫短期証券を検出")
    print()
    
    items = parser.extract(text)
    
    print(f"✅ パース完了: {len(items)} 項目抽出")
    print()
    
    print("-" * 70)
    print("📊 抽出結果サマリー")
    print("-" * 70)
    
    if 1 in items:
        bond_info = items[1]['structured_data']
        print(f"銘柄: {bond_info.get('bond_name', 'N/A')} {bond_info.get('bond_series', 'N/A')}")
    
    if 6 in items:
        total_amount = items[6]['structured_data'].get('total_amount', 0)
        print(f"総発行額: {total_amount:,}円（{total_amount/1_000_000_000_000:.2f}兆円）")
    
    if 10 in items:
        print(f"発行日: {items[10]['structured_data'].get('issue_date', 'N/A')}")
    
    if 12 in items:
        print(f"償還期限: {items[12]['structured_data'].get('issue_date', 'N/A')}")
    
    print()
    
    # 根拠法令別の発行額を集計
    if 2 in items and 6 in items:
        print("-" * 70)
        print("💰 根拠法令別の発行額集計")
        print("-" * 70)
        
        law_totals = {}
        issue_data = items[6]['structured_data']
        
        for method in ['competitive', 'noncompetitive1']:
            if method in issue_data and 'by_law' in issue_data[method]:
                for law_key, amount in issue_data[method]['by_law'].items():
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
    
    print("-" * 70)
    print("📤 BigQuery へデータ投入")
    print("-" * 70)
    
    announcement_id = "20231211_16"
    success = insert_to_bigquery(announcement_id, text, items, file_path)
    
    if success:
        print()
        print("=" * 70)
        print("✅ テスト完了！")
        print("=" * 70)
        print(f"Announcement ID: {announcement_id}")
        print(f"Layer 1: raw_announcements に 1行 投入")
        print(f"Layer 2: announcement_items に {len(items)}行 投入")
    else:
        print()
        print("❌ データ投入に失敗しました")
    
    return items


if __name__ == "__main__":
    test_tb()