"""
Phase 4 Prototype: Universal Parser v4
添付の告示（20230915）でテスト

3層アーキテクチャにデータを投入：
- Layer 1: raw_announcements（告示テーブル）
- Layer 2: announcement_items（正規化テーブル）
- Layer 3: ビュー（後で作成）
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


class NumberedListExtractor:
    """番号付きリスト（1～20）を抽出"""
    
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
        
        # 項目2: 発行の根拠法律及びその条項
        match = re.search(r'２\s+発行の根拠法律及びその条項\s+(.+?)(?=\n３)', text, re.DOTALL)
        if match:
            items[2] = {
                'title': '発行の根拠法律及びその条項',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_item2(match.group(1))
            }
        
        # 項目6: 発行額（複雑なので詳細に）
        match = re.search(r'６\s+発行額(.+?)(?=\n７)', text, re.DOTALL)
        if match:
            full_text = match.group(1).strip()
            items[6] = {
                'title': '発行額',
                'value': full_text,
                'sub_number': None,
                'structured_data': self._parse_item6(full_text)
            }
            
            # サブ項目も抽出（⑴⑵⑶）
            sub_items = self._extract_sub_items(full_text)
            for sub_num, sub_data in sub_items.items():
                items[f'6_{sub_num}'] = {
                    'title': f'発行額⑴⑵⑶',
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
                'structured_data': self._parse_item10(match.group(1))
            }
        
        # 項目12: 利率
        match = re.search(r'12\s+利率\s+(.+?)(?=\n13)', text, re.DOTALL)
        if match:
            items[12] = {
                'title': '利率',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_item12(match.group(1))
            }
        
        # 項目16: 償還期限
        match = re.search(r'16\s+償還期限\s+(.+?)(?=\n17)', text, re.DOTALL)
        if match:
            items[16] = {
                'title': '償還期限',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_item16(match.group(1))
            }
        
        # 項目17: 償還金額
        match = re.search(r'17\s+償還金額\s+(.+?)(?=\n18)', text, re.DOTALL)
        if match:
            items[17] = {
                'title': '償還金額',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_item17(match.group(1))
            }
        
        return items
    
    def _extract_sub_items(self, text: str) -> dict:
        """⑴⑵⑶のサブ項目を抽出"""
        sub_items = {}
        
        # ⑴を抽出
        match1 = re.search(r'⑴\s*価格競争入札発行(.+?)(?=⑵|$)', text, re.DOTALL)
        if match1:
            sub_items[1] = {
                'value': f'⑴ 価格競争入札発行{match1.group(1).strip()}',
                'structured': self._parse_sub_item_amount(match1.group(1))
            }
        
        # ⑵を抽出
        match2 = re.search(r'⑵\s*国債市場特別参加者・第Ⅰ非価格競争入札発行(.+?)(?=⑶|$)', text, re.DOTALL)
        if match2:
            sub_items[2] = {
                'value': f'⑵ 国債市場特別参加者・第Ⅰ非価格競争入札発行{match2.group(1).strip()}',
                'structured': self._parse_sub_item_amount(match2.group(1))
            }
        
        # ⑶を抽出
        match3 = re.search(r'⑶\s*国債市場特別参加者・第Ⅱ非価格競争入札発行(.+?)$', text, re.DOTALL)
        if match3:
            sub_items[3] = {
                'value': f'⑶ 国債市場特別参加者・第Ⅱ非価格競争入札発行{match3.group(1).strip()}',
                'structured': self._parse_sub_item_amount(match3.group(1))
            }
        
        return sub_items
    
    def _parse_sub_item_amount(self, text: str) -> dict:
        """サブ項目から発行額を抽出（「同法」対応版）"""
        result = {'total_amount': 0, 'by_law': {}}
        
        # 総額を抽出
        total_match = re.search(r'額面金額で([\d,]+)円', text)
        if total_match:
            result['total_amount'] = int(total_match.group(1).replace(',', ''))
        
        # 法令別の内訳
        last_law_name = None
        
        # 明示的な法令名のマッチ
        law_matches = re.finditer(
            r'([^、]+第\d+条第\d+項)の規定に基づき.*?額面金額で([\d,]+)円',
            text
        )
        
        for match in law_matches:
            law_ref = match.group(1).strip()
            amount = int(match.group(2).replace(',', ''))
            
            # 法律名を正規化
            law_key = self._normalize_law_name(law_ref)
            if law_key:
                result['by_law'][law_key] = amount
                
                # 法律名を記憶
                if '特別会計' in law_ref:
                    last_law_name = '特別会計に関する法律'
                elif '財政運営' in law_ref:
                    last_law_name = '財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律'
                elif '財政法' in law_ref:
                    last_law_name = '財政法'
        
        # 「同法第○条第○項」のマッチ
        same_law_matches = re.finditer(
            r'同法第(\d+)条第(\d+)項の規定に基づき.*?額面金額で([\d,]+)円',
            text
        )
        
        for match in same_law_matches:
            article = match.group(1)
            paragraph = match.group(2)
            amount = int(match.group(3).replace(',', ''))
            
            if last_law_name:
                if last_law_name == '特別会計に関する法律':
                    if article == '46':
                        law_key = '特別会計に関する法律第46条第1項'
                    elif article == '62':
                        law_key = '特別会計に関する法律第62条第1項'
                    else:
                        law_key = f'{last_law_name}第{article}条第{paragraph}項'
                elif last_law_name == '財政法':
                    law_key = f'財政法第{article}条第{paragraph}項'
                elif last_law_name == '財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律':
                    law_key = f'財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律第{article}条第{paragraph}項'
                else:
                    law_key = f'{last_law_name}第{article}条第{paragraph}項'
                
                result['by_law'][law_key] = amount
        
        return result
    
    def _normalize_law_name(self, law_ref: str) -> str:
        """法律名を正規化"""
        if '財政法' in law_ref and '第４条' in law_ref:
            return '財政法第4条第1項'
        elif '財政運営' in law_ref:
            return '財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律第3条第1項'
        elif '特別会計' in law_ref:
            if '第46条' in law_ref:
                return '特別会計に関する法律第46条第1項'
            elif '第62条' in law_ref:
                return '特別会計に関する法律第62条第1項'
        return None
    
    def _parse_item1(self, text: str) -> dict:
        """項目1: 名称及び記号を構造化"""
        match = re.search(r'利\s*付国庫債券[（\(](.+?)[）\)][（\(]第(\d+)回[）\)]', text)
        if match:
            return {
                'bond_name': f'利付国庫債券（{match.group(1)}）',
                'bond_series': f'第{match.group(2)}回'
            }
        return {'raw': text}
    
    def _parse_item2(self, text: str) -> dict:
        """項目2: 発行の根拠法律を構造化（改善版 + デバッグ）"""
        
        laws = []
        
        # 全角数字を半角に変換
        text_normalized = text.replace('４', '4').replace('３', '3').replace('１', '1')
        
        print(f"    [DEBUG] 項目2の元テキスト: {text[:100]}...")
        print(f"    [DEBUG] 正規化後: {text_normalized[:100]}...")
        
        # パターン1: 財政法
        if re.search(r'財政法.*?第4条第1項', text_normalized, re.DOTALL):
            print("    [DEBUG] ✅ 財政法を検出")
            laws.append({
                'name': '財政法',
                'full_name': '財政法（昭和22年法律第34号）',
                'article': '第4条第1項',
                'key': '財政法第4条第1項',
                'amount': 0
            })
        else:
            print("    [DEBUG] ❌ 財政法を検出できず")
        
        # パターン2: 財政運営（公債特例法）
        if re.search(r'財政運営.*?第3条第1項', text_normalized, re.DOTALL):
            print("    [DEBUG] ✅ 財政運営法を検出")
            laws.append({
                'name': '財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律',
                'full_name': '財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律（平成24年法律第101号）',
                'article': '第3条第1項',
                'key': '財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律第3条第1項',
                'amount': 0
            })
        else:
            print("    [DEBUG] ❌ 財政運営法を検出できず")
        
        # パターン3: 特別会計法第46条
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
        
        # パターン4: 特別会計法第62条
        if re.search(r'特別会計.*?第62条第1項', text_normalized, re.DOTALL):
            print("    [DEBUG] ✅ 特別会計法第62条を検出")
            laws.append({
                'name': '特別会計に関する法律',
                'full_name': '特別会計に関する法律（平成19年法律第23号）',
                'article': '第62条第1項',
                'key': '特別会計に関する法律第62条第1項',
                'amount': 0
            })
        else:
            print("    [DEBUG] ❌ 特別会計法第62条を検出できず")
        
        print(f"    [DEBUG] 最終的に抽出された法令数: {len(laws)}")
        
        return {'laws': laws}
    
    def _parse_item6(self, text: str) -> dict:
        """項目6: 発行額を構造化"""
        
        result = {
            'total_amount': 0,
            'competitive': {'amount': 0, 'by_law': {}},
            'noncompetitive1': {'amount': 0, 'by_law': {}},
            'noncompetitive2': {'amount': 0, 'by_law': {}}
        }
        
        # ⑴ 価格競争入札発行
        comp_match = re.search(r'⑴\s*価格競争入札発行\s*額面金額で([\d,]+)円', text)
        if comp_match:
            amount = int(comp_match.group(1).replace(',', ''))
            result['competitive']['amount'] = amount
            result['total_amount'] += amount
            
            # 法令別の内訳
            comp_section = re.search(r'⑴(.+?)(?=⑵|$)', text, re.DOTALL)
            if comp_section:
                result['competitive']['by_law'] = self._extract_law_amounts_from_section(comp_section.group(1))
        
        # ⑵ 第Ⅰ非価格競争入札発行
        nc1_match = re.search(r'⑵.*?額面金額で([\d,]+)円', text, re.DOTALL)
        if nc1_match:
            amount = int(nc1_match.group(1).replace(',', ''))
            result['noncompetitive1']['amount'] = amount
            result['total_amount'] += amount
            
            nc1_section = re.search(r'⑵(.+?)(?=⑶|$)', text, re.DOTALL)
            if nc1_section:
                result['noncompetitive1']['by_law'] = self._extract_law_amounts_from_section(nc1_section.group(1))
        
        # ⑶ 第Ⅱ非価格競争入札発行
        nc2_match = re.search(r'⑶.*?額面金額で([\d,]+)円', text, re.DOTALL)
        if nc2_match:
            amount = int(nc2_match.group(1).replace(',', ''))
            result['noncompetitive2']['amount'] = amount
            result['total_amount'] += amount
            
            nc2_section = re.search(r'⑶(.+?)$', text, re.DOTALL)
            if nc2_section:
                result['noncompetitive2']['by_law'] = self._extract_law_amounts_from_section(nc2_section.group(1))
        
        return result
    
    def _extract_law_amounts_from_section(self, section: str) -> dict:
        """セクションから法令別の発行額を抽出（「同法」対応版）"""
        by_law = {}
        
        # まず、明示的な法令名のパターンをマッチ
        law_matches = re.finditer(
            r'([^、]+第\d+条第\d+項)の規定に基づき.*?額面金額で([\d,]+)円',
            section
        )
        
        last_law_name = None  # 直前の法律名を記憶
        
        for match in law_matches:
            law_ref = match.group(1).strip()
            amount = int(match.group(2).replace(',', ''))
            
            # 法律名を正規化
            law_key = self._normalize_law_name(law_ref)
            if law_key:
                by_law[law_key] = amount
                
                # 法律名を記憶（「同法」用）
                if '特別会計' in law_ref:
                    last_law_name = '特別会計に関する法律'
                elif '財政運営' in law_ref:
                    last_law_name = '財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律'
                elif '財政法' in law_ref:
                    last_law_name = '財政法'
        
        # 「同法第○条第○項」のパターンをマッチ
        same_law_matches = re.finditer(
            r'同法第(\d+)条第(\d+)項の規定に基づき.*?額面金額で([\d,]+)円',
            section
        )
        
        for match in same_law_matches:
            article = match.group(1)
            paragraph = match.group(2)
            amount = int(match.group(3).replace(',', ''))
            
            # 直前の法律名を使用
            if last_law_name:
                if last_law_name == '特別会計に関する法律':
                    if article == '46':
                        law_key = '特別会計に関する法律第46条第1項'
                    elif article == '62':
                        law_key = '特別会計に関する法律第62条第1項'
                    else:
                        law_key = f'{last_law_name}第{article}条第{paragraph}項'
                elif last_law_name == '財政法':
                    law_key = f'財政法第{article}条第{paragraph}項'
                elif last_law_name == '財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律':
                    law_key = f'財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律第{article}条第{paragraph}項'
                else:
                    law_key = f'{last_law_name}第{article}条第{paragraph}項'
                
                by_law[law_key] = amount
                print(f"    [DEBUG] 「同法」を展開: {law_key} = {amount:,}円")
        
        return by_law
    
    def _parse_item10(self, text: str) -> dict:
        """項目10: 発行日"""
        match = re.search(r'令和(\d+)年(\d+)月(\d+)日', text)
        if match:
            year = int(match.group(1)) + 2018
            month = int(match.group(2))
            day = int(match.group(3))
            return {'issue_date': f'{year}-{month:02d}-{day:02d}'}
        return {'raw': text}
    
    def _parse_item12(self, text: str) -> dict:
        """項目12: 利率"""
        match = re.search(r'年([\d.]+)％', text)
        if match:
            return {'rate': float(match.group(1))}
        return {'raw': text}
    
    def _parse_item16(self, text: str) -> dict:
        """項目16: 償還期限"""
        match = re.search(r'令和(\d+)年(\d+)月(\d+)日', text)
        if match:
            year = int(match.group(1)) + 2018
            month = int(match.group(2))
            day = int(match.group(3))
            return {'maturity_date': f'{year}-{month:02d}-{day:02d}'}
        return {'raw': text}
    
    def _parse_item17(self, text: str) -> dict:
        """項目17: 償還金額"""
        match = re.search(r'額面金額100円につき(\d+)円', text)
        if match:
            return {'amount': int(match.group(1))}
        return {'raw': text}


def insert_to_bigquery(announcement_id: str, raw_text: str, items: dict, file_path: Path):
    """BigQueryにデータを投入"""
    
    client = bigquery.Client(project=PROJECT_ID)
    
    # 基本情報の抽出
    issue_date = items.get(10, {}).get('structured_data', {}).get('issue_date', '2023-09-15')
    announcement_date = '2023-10-11'  # 告示日（ファイル名から）
    announcement_number = '第二百五十一号'
    
    # Layer 1: raw_announcements に投入
    raw_row = {
        'announcement_id': announcement_id,
        'announcement_date': announcement_date,
        'announcement_number': announcement_number,
        'issue_date': issue_date,
        'format_pattern': 'NUMBERED_LIST_MULTI_LAW',
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
    
    # Layer 2: announcement_items に投入
    item_rows = []
    for key, item_data in items.items():
        # キーが文字列（"6_1"など）の場合、item_numberとsub_numberに分割
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


def prototype_test():
    """プロトタイプテスト：添付の告示を処理"""
    
    # ファイル読み込み
    file_path = Path(r"G:\マイドライブ\JGBデータ\2023\20230915_令和5年10月11日付（財務省第二百五十一号）.txt")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
    
    print("=" * 70)
    print("Phase 4 プロトタイプテスト: 添付の告示をパース")
    print("=" * 70)
    print(f"📄 ファイル: {file_path.name}")
    print(f"📝 文字数: {len(text):,}")
    print()
    
    # パース実行
    extractor = NumberedListExtractor()
    items = extractor.extract(text)
    
    print(f"✅ パース完了: {len(items)} 項目抽出")
    print()
    
    # 結果表示（主要項目のみ）
    print("-" * 70)
    print("📊 抽出結果サマリー")
    print("-" * 70)
    
    if 1 in items:
        bond_info = items[1]['structured_data']
        print(f"銘柄: {bond_info.get('bond_name', 'N/A')} {bond_info.get('bond_series', 'N/A')}")
    
    if 10 in items:
        print(f"発行日: {items[10]['structured_data'].get('issue_date', 'N/A')}")
    
    if 12 in items:
        print(f"利率: {items[12]['structured_data'].get('rate', 'N/A')}%")
    
    if 16 in items:
        print(f"償還期限: {items[16]['structured_data'].get('maturity_date', 'N/A')}")
    
    print()
    
    # 法令別の発行額を集計
    if 2 in items and 6 in items:
        print("-" * 70)
        print("💰 根拠法令別の発行額集計")
        print("-" * 70)
        
        laws = items[2]['structured_data']['laws']
        issue_data = items[6]['structured_data']
        
        # 各法令の発行額を計算
        law_totals = {}
        
        for method in ['competitive', 'noncompetitive1', 'noncompetitive2']:
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
    
    # BigQueryへ投入
    print("-" * 70)
    print("📤 BigQuery へデータ投入")
    print("-" * 70)
    
    # バージョン番号を付けて重複を避ける
    announcement_id = "20230915_251_v3"
    success = insert_to_bigquery(announcement_id, text, items, file_path)
    
    if success:
        print()
        print("=" * 70)
        print("✅ プロトタイプテスト完了！")
        print("=" * 70)
        print(f"Announcement ID: {announcement_id}")
        print(f"Layer 1: raw_announcements に 1行 投入")
        print(f"Layer 2: announcement_items に {len(items)}行 投入")
        print()
        print("次のステップ: BigQueryコンソールでデータを確認してください")
        print(f"  SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.raw_announcements` LIMIT 10;")
        print(f"  SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.announcement_items` LIMIT 10;")
    else:
        print()
        print("❌ データ投入に失敗しました")
    
    return items


if __name__ == "__main__":
    prototype_test()