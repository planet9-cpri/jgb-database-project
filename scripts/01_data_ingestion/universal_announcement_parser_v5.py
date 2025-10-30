"""
Phase 5 統合パーサー: Universal Announcement Parser (完成版)
4つのパーサーを完全統合、BigQuery投入、バッチ処理機能を実装
"""

import re
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
import os
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json'

PROJECT_ID = "jgb2023"
DATASET_ID = "20251028"


class NumberedListParser:
    """番号付きリスト形式（複数法令対応 + シンプル単一銘柄対応）"""
    
    def can_parse(self, text: str) -> float:
        """処理可能性スコア（0.0～1.0）"""
        score = 0.0
        
        # 国庫短期証券の場合はスコアを大幅に下げる
        if '国庫短期証券' in text or '割引短期国債' in text or '政府短期証券' in text:
            return 0.1  # TBParserに優先させる
        
        # 基本的な番号付きリスト構造の検出（シンプルな告示用）
        # 全角スペースの数に柔軟に対応するため、スペース部分を正規表現で扱う
        has_item1 = bool(re.search(r'１\s+名称及び記号', text))
        has_item2 = bool(re.search(r'２.*?発行の根拠法律', text, re.DOTALL))
        has_item_amount = bool(re.search(r'[５６]\s+.*?発.*?行.*?額', text))
        
        has_basic_structure = has_item1 and has_item2 and has_item_amount
        
        if has_basic_structure:
            score += 0.4  # 基本構造があれば0.4点
        
        # 価格競争入札 + 非価格競争入札（複雑なパターン）
        if '価格競争入札' in text and '非価格競争入札' in text:
            score += 0.3
        
        # 複数法令（並びに、及び）（複雑なパターン）
        if '並びに' in text and '及び' in text:
            score += 0.2
        
        # 別表がない（番号付きリストの特徴）
        if '（別表のとおり）' not in text and '別表' not in text:
            score += 0.3
        
        return min(score, 1.0)
    
    def extract(self, text: str) -> Dict[str, Any]:
        """データ抽出"""
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
                    'title': '発行額⑴⑵⑶',
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
                'structured_data': self._parse_date(match.group(1))
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
        
        return {'pattern': 'NUMBERED_LIST_MULTI_LAW', 'items': items}
    
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
                
                # 法律名を記憶（「同法」用）
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
                else:
                    law_key = f'{last_law_name}第{article}条第{paragraph}項'
                
                result['by_law'][law_key] = amount
        
        return result
    
    def _normalize_law_name(self, law_ref: str) -> str:
        """法律名を正規化"""
        if '財政法' in law_ref and '第４条' in law_ref or '第4条' in law_ref:
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
        """項目2: 発行の根拠法律を構造化"""
        laws = []
        text_normalized = text.replace('４', '4').replace('３', '3').replace('１', '1')
        
        # 財政法第4条第1項
        if re.search(r'財政法.*?第4条第1項', text_normalized, re.DOTALL):
            laws.append({
                'name': '財政法',
                'full_name': '財政法（昭和22年法律第34号）',
                'article': '第4条第1項',
                'key': '財政法第4条第1項',
                'amount': 0
            })
        
        # 財政運営法第3条第1項
        if re.search(r'財政運営.*?第3条第1項', text_normalized, re.DOTALL):
            laws.append({
                'name': '財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律',
                'full_name': '財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律（平成24年法律第101号）',
                'article': '第3条第1項',
                'key': '財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律第3条第1項',
                'amount': 0
            })
        
        # 特別会計法第46条第1項
        if re.search(r'特別会計.*?第46条第1項', text_normalized, re.DOTALL):
            laws.append({
                'name': '特別会計に関する法律',
                'full_name': '特別会計に関する法律（平成19年法律第23号）',
                'article': '第46条第1項',
                'key': '特別会計に関する法律第46条第1項',
                'amount': 0
            })
        
        # 特別会計法第62条第1項
        if re.search(r'特別会計.*?第62条第1項', text_normalized, re.DOTALL):
            laws.append({
                'name': '特別会計に関する法律',
                'full_name': '特別会計に関する法律（平成19年法律第23号）',
                'article': '第62条第1項',
                'key': '特別会計に関する法律第62条第1項',
                'amount': 0
            })
        
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
        
        # 明示的な法令名のパターンをマッチ
        law_matches = re.finditer(
            r'([^、]+第\d+条第\d+項)の規定に基づき.*?額面金額で([\d,]+)円',
            section
        )
        
        last_law_name = None
        
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
            
            if last_law_name:
                if last_law_name == '特別会計に関する法律':
                    if article == '46':
                        law_key = '特別会計に関する法律第46条第1項'
                    elif article == '62':
                        law_key = '特別会計に関する法律第62条第1項'
                    else:
                        law_key = f'{last_law_name}第{article}条第{paragraph}項'
                else:
                    law_key = f'{last_law_name}第{article}条第{paragraph}項'
                
                by_law[law_key] = amount
        
        return by_law
    
    def _parse_date(self, text: str) -> dict:
        """日付を解析"""
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
    
    def _parse_item17(self, text: str) -> dict:
        """項目17: 償還金額"""
        match = re.search(r'額面金額100円につき(\d+)円', text)
        if match:
            return {'amount': int(match.group(1))}
        return {'raw': text}


class TableParserV4:
    """横並び別表形式の告示を解析"""
    
    def can_parse(self, text: str) -> float:
        """処理可能性スコア（0.0～1.0）"""
        score = 0.0
        
        # 別表の存在
        if '（別表のとおり）' in text or '内訳（別表のとおり）' in text:
            score += 0.4
        
        # 別表の実データ
        if '名称及び記号' in text and '利率' in text and '償還期限' in text:
            score += 0.4
        
        # 別表ヘッダーの存在
        if '（別表）' in text:
            score += 0.2
        
        return min(score, 1.0)
    
    def extract(self, text: str) -> Dict[str, Any]:
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
            # 別表の各行を項目6のサブ項目として追加
            for i, row in enumerate(table_data, start=1):
                items[f'6_{i}'] = {
                    'title': '発行額（別表）',
                    'value': self._format_table_row(row),
                    'sub_number': i,
                    'structured_data': row
                }
        
        return {'pattern': 'TABLE_HORIZONTAL', 'items': items}
    
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
        
        # 別表の行パターン
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


class RetailBondParser:
    """個人向け国債の告示を解析"""
    
    def can_parse(self, text: str) -> float:
        """処理可能性スコア（0.0～1.0）"""
        if '個人向け利付国庫債券' in text or '個人向け国債' in text:
            return 1.0
        return 0.0
    
    def extract(self, text: str) -> Dict[str, Any]:
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
        
        return {'pattern': 'RETAIL_BOND', 'items': items}
    
    def _parse_item1(self, text: str) -> dict:
        """項目1: 名称及び記号を構造化"""
        match = re.search(r'個人向け利付国庫債券[（\(](.+?)[）\)][（\(]第(\d+)回[）\)]', text)
        if match:
            return {
                'bond_name': f'個人向け利付国庫債券（{match.group(1)}）',
                'bond_series': f'第{match.group(2)}回',
                'bond_type': match.group(1)
            }
        return {'raw': text}
    
    def _parse_laws(self, text: str) -> dict:
        """法令を抽出"""
        laws = []
        text_normalized = text.replace('４', '4').replace('６', '6').replace('１', '1')
        
        if re.search(r'特別会計.*?第46条第1項', text_normalized, re.DOTALL):
            laws.append({
                'name': '特別会計に関する法律',
                'full_name': '特別会計に関する法律（平成19年法律第23号）',
                'article': '第46条第1項',
                'key': '特別会計に関する法律第46条第1項',
                'amount': 0
            })
        
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


class TBParser:
    """国庫短期証券（Treasury Bill）の告示を解析"""
    
    def can_parse(self, text: str) -> float:
        """処理可能性スコア（0.0～1.0）"""
        score = 0.0
        
        if '国庫短期証券' in text:
            score += 0.5
        
        if '割引短期国債' in text or '政府短期証券' in text:
            score += 0.5
        
        return min(score, 1.0)
    
    def extract(self, text: str) -> Dict[str, Any]:
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
        
        return {'pattern': 'TB_SHORT_TERM', 'items': items}
    
    def _parse_item1(self, text: str) -> dict:
        """項目1: 名称及び記号を構造化"""
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
        
        # 特別会計法第46条第1項
        if re.search(r'特別会計.*?第46条第1項', text_normalized, re.DOTALL):
            laws.append({
                'name': '特別会計に関する法律',
                'full_name': '特別会計に関する法律（平成19年法律第23号）',
                'article': '第46条第1項',
                'key': '特別会計に関する法律第46条第1項',
                'amount': 0
            })
        
        # 政府短期証券用の法令グループ
        tb_articles = ['83', '94', '95', '136', '137']
        if any(f'第{a}条' in text_normalized for a in tb_articles):
            laws.append({
                'name': '特別会計に関する法律（政府短期証券）',
                'full_name': '特別会計に関する法律（複数条項）',
                'article': '第83条等',
                'key': '政府短期証券関連法令',
                'amount': 0
            })
        
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
                'value': f'⑴ 価格競争入札発行 額面金額で{match1.group(1)}円',
                'structured': self._parse_sub_item_amount(detail, total_amount)
            }
        
        # ⑵ 第Ⅰ非価格競争入札発行
        match2 = re.search(r'⑵\s*国債市場特別参加者・第Ⅰ非価格競争入札発行(.+?)$', text, re.DOTALL)
        if match2:
            detail = match2.group(1)
            amount_match = re.search(r'額面金額で([\d,]+)円', detail)
            if amount_match:
                total_amount = int(amount_match.group(1).replace(',', ''))
                sub_items[2] = {
                    'value': f'⑵ 国債市場特別参加者・第Ⅰ非価格競争入札発行',
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
        
        # 政府短期証券（複数法令）
        gsb_match = re.search(r'財政法第７条第１項.*?政府短期証券.*?額面金額で([\d,]+)円', text, re.DOTALL)
        if gsb_match:
            amount = int(gsb_match.group(1).replace(',', ''))
            result['by_law']['政府短期証券関連法令'] = amount
        
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


class UniversalAnnouncementParser:
    """統合パーサー：すべてのパターンに対応（完成版）"""
    
    def __init__(self):
        # 4つのパーサーを登録（優先順位順）
        self.parsers = [
            ('RETAIL_BOND', RetailBondParser()),
            ('TABLE_HORIZONTAL', TableParserV4()),
            ('TB_SHORT_TERM', TBParser()),
            ('NUMBERED_LIST_MULTI_LAW', NumberedListParser()),
        ]
        
        self.client = bigquery.Client(project=PROJECT_ID)
    
    def identify_pattern(self, text: str) -> Tuple[str, float]:
        """
        パターンを自動識別
        
        Returns:
            (pattern_name, confidence_score)
        """
        scores = {}
        
        for pattern_name, parser in self.parsers:
            score = parser.can_parse(text)
            scores[pattern_name] = score
        
        # 最高スコアのパターンを選択
        best_pattern = max(scores.items(), key=lambda x: x[1])
        
        return best_pattern[0], best_pattern[1]
    
    def parse(self, text: str, file_path: Optional[Path] = None) -> Dict[str, Any]:
        """
        告示を解析
        
        Returns:
            {
                'pattern': パターン名,
                'confidence': 信頼度,
                'items': 抽出データ,
                'error': エラーメッセージ（あれば）,
                'file_path': ファイルパス
            }
        """
        try:
            # パターン識別
            pattern, confidence = self.identify_pattern(text)
            
            # 信頼度が低すぎる場合は警告
            if confidence < 0.5:
                return {
                    'pattern': 'UNKNOWN',
                    'confidence': confidence,
                    'items': {},
                    'error': f'パターン識別の信頼度が低い（{confidence:.2f}）',
                    'file_path': str(file_path) if file_path else None
                }
            
            # 適切なパーサーを選択
            parser = None
            for p_name, p in self.parsers:
                if p_name == pattern:
                    parser = p
                    break
            
            if parser is None:
                return {
                    'pattern': pattern,
                    'confidence': confidence,
                    'items': {},
                    'error': f'パーサーが見つかりません: {pattern}',
                    'file_path': str(file_path) if file_path else None
                }
            
            # データ抽出
            result = parser.extract(text)
            
            return {
                'pattern': result['pattern'],
                'confidence': confidence,
                'items': result.get('items', {}),
                'error': None,
                'file_path': str(file_path) if file_path else None
            }
        
        except Exception as e:
            return {
                'pattern': 'ERROR',
                'confidence': 0.0,
                'items': {},
                'error': str(e),
                'file_path': str(file_path) if file_path else None
            }
    
    def extract_metadata_from_filename(self, file_path: Path) -> Dict[str, str]:
        """ファイル名からメタデータを抽出"""
        filename = file_path.stem
        
        # ファイル名のパターン: 20230915_令和5年10月11日付（財務省第二百五十一号）
        match = re.match(r'(\d{8})_令和(\d+)年(\d+)月(\d+)日付（財務省(.+?)）', filename)
        
        if match:
            file_date = match.group(1)
            reiwa_year = int(match.group(2))
            month = int(match.group(3))
            day = int(match.group(4))
            announcement_num = match.group(5)
            
            announcement_year = reiwa_year + 2018
            announcement_date = f'{announcement_year}-{month:02d}-{day:02d}'
            
            return {
                'file_date': file_date,
                'announcement_date': announcement_date,
                'announcement_number': announcement_num
            }
        
        return {
            'file_date': 'unknown',
            'announcement_date': 'unknown',
            'announcement_number': 'unknown'
        }
    
    def insert_to_bigquery(
        self,
        announcement_id: str,
        raw_text: str,
        items: dict,
        file_path: Path,
        pattern: str,
        confidence: float
    ) -> bool:
        """BigQueryにデータを投入"""
        
        try:
            # ファイル名からメタデータ抽出
            metadata = self.extract_metadata_from_filename(file_path)
            
            # 発行日を抽出（項目10または項目7）
            issue_date = None
            if 10 in items:
                issue_date = items[10].get('structured_data', {}).get('issue_date')
            elif 7 in items:
                issue_date = items[7].get('structured_data', {}).get('issue_date')
            
            if not issue_date:
                issue_date = metadata['announcement_date']
            
            # Layer 1: raw_announcements に投入
            raw_row = {
                'announcement_id': announcement_id,
                'announcement_date': metadata['announcement_date'],
                'announcement_number': metadata['announcement_number'],
                'issue_date': issue_date,
                'format_pattern': pattern,
                'format_pattern_confidence': confidence,
                'raw_text': raw_text,
                'source_file': file_path.name,
                'file_path': str(file_path),
                'parsed': True,
                'parse_error': None,
                'parsed_at': datetime.now().isoformat()
            }
            
            table_ref = f"{PROJECT_ID}.{DATASET_ID}.raw_announcements"
            errors = self.client.insert_rows_json(table_ref, [raw_row])
            
            if errors:
                print(f"  ❌ Layer 1 投入エラー: {errors}")
                return False
            
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
            errors = self.client.insert_rows_json(table_ref, item_rows)
            
            if errors:
                print(f"  ❌ Layer 2 投入エラー: {errors}")
                return False
            
            return True
        
        except Exception as e:
            print(f"  ❌ BigQuery投入例外: {e}")
            return False


def batch_process(
    input_dir: Path,
    dataset_id: str = DATASET_ID,
    test_mode: bool = True,
    max_files: int = 10,
    insert_to_bq: bool = True
) -> Dict[str, Any]:
    """
    バッチ処理：複数ファイルを一括処理
    
    Args:
        input_dir: 入力ディレクトリ
        dataset_id: BigQueryデータセットID
        test_mode: テストモード（Trueで最大max_files件）
        max_files: テストモード時の最大処理件数
        insert_to_bq: BigQueryに投入するか
    
    Returns:
        処理結果の統計情報
    """
    parser = UniversalAnnouncementParser()
    
    # ファイル一覧取得
    files = sorted(input_dir.glob('*.txt'))
    
    if test_mode:
        files = files[:max_files]
        print(f"🧪 テストモード: 最初の{len(files)}ファイルを処理")
    else:
        print(f"🚀 本番モード: {len(files)}ファイルを処理")
    
    print(f"📤 BigQuery投入: {'有効' if insert_to_bq else '無効'}")
    print()
    
    # 統計情報
    stats = {
        'total': len(files),
        'success': 0,
        'failed': 0,
        'by_pattern': {},
        'errors': [],
        'inserted': 0
    }
    
    for i, file_path in enumerate(files, 1):
        print(f"[{i}/{len(files)}] {file_path.name}")
        
        try:
            # ファイル読み込み
            with open(file_path, 'r', encoding='utf-8') as f:
                text = f.read()
            
            # パース
            result = parser.parse(text, file_path)
            
            pattern = result['pattern']
            confidence = result['confidence']
            
            print(f"  パターン: {pattern} (信頼度: {confidence:.2f})")
            
            # 統計更新
            if pattern not in stats['by_pattern']:
                stats['by_pattern'][pattern] = 0
            stats['by_pattern'][pattern] += 1
            
            if result['error']:
                print(f"  ❌ エラー: {result['error']}")
                stats['failed'] += 1
                stats['errors'].append({
                    'file': file_path.name,
                    'pattern': pattern,
                    'error': result['error']
                })
            else:
                print(f"  ✅ パース成功")
                stats['success'] += 1
                
                # BigQuery投入
                if insert_to_bq:
                    # announcement_idを生成（ファイル名から）
                    match = re.match(r'(\d{8})_.*（財務省(.+?)）', file_path.name)
                    if match:
                        date_part = match.group(1)
                        num_part = match.group(2).replace('第', '').replace('号', '')
                        announcement_id = f"{date_part}_{num_part}"
                    else:
                        announcement_id = file_path.stem
                    
                    success = parser.insert_to_bigquery(
                        announcement_id,
                        text,
                        result['items'],
                        file_path,
                        pattern,
                        confidence
                    )
                    
                    if success:
                        print(f"  ✅ BigQuery投入成功")
                        stats['inserted'] += 1
                    else:
                        print(f"  ❌ BigQuery投入失敗")
            
            print()
        
        except Exception as e:
            print(f"  ❌ 例外: {e}")
            stats['failed'] += 1
            stats['errors'].append({
                'file': file_path.name,
                'pattern': 'EXCEPTION',
                'error': str(e)
            })
            print()
    
    # 結果サマリー
    print("=" * 70)
    print("📊 処理結果サマリー")
    print("=" * 70)
    print(f"総ファイル数: {stats['total']}")
    print(f"成功: {stats['success']} ({stats['success']/stats['total']*100:.1f}%)")
    print(f"失敗: {stats['failed']} ({stats['failed']/stats['total']*100:.1f}%)")
    if insert_to_bq:
        print(f"BigQuery投入: {stats['inserted']} ({stats['inserted']/stats['total']*100:.1f}%)")
    print()
    
    print("パターン別:")
    for pattern, count in sorted(stats['by_pattern'].items(), key=lambda x: x[1], reverse=True):
        print(f"  {pattern}: {count} ({count/stats['total']*100:.1f}%)")
    print()
    
    if stats['errors']:
        print(f"エラー詳細（最初の5件）:")
        for error in stats['errors'][:5]:
            print(f"  - {error['file']}: {error['error'][:50]}...")
        print()
    
    return stats


def test_universal_parser():
    """統合パーサーのテスト（4ファイル）"""
    
    print("=" * 70)
    print("Phase 5 統合パーサー テスト")
    print("=" * 70)
    print()
    
    # テストケース
    test_files = [
        Path(r"G:\マイドライブ\JGBデータ\2023\20230915_令和5年10月11日付（財務省第二百五十一号）.txt"),
        Path(r"G:\マイドライブ\JGBデータ\2023\20230414_令和5年5月9日付（財務省第百二十七号）.txt"),
        Path(r"G:\マイドライブ\JGBデータ\2023\20230615_令和5年7月11日付（財務省第百九十二号）.txt"),
        Path(r"G:\マイドライブ\JGBデータ\2023\20231211_令和6年1月12日付（財務省第十六号）.txt"),
    ]
    
    parser = UniversalAnnouncementParser()
    
    for file_path in test_files:
        if not file_path.exists():
            print(f"⚠️  ファイルが見つかりません: {file_path.name}")
            continue
        
        print(f"📄 {file_path.name}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()
        
        result = parser.parse(text, file_path)
        
        print(f"  パターン: {result['pattern']}")
        print(f"  信頼度: {result['confidence']:.2f}")
        
        if result['error']:
            print(f"  ❌ エラー: {result['error']}")
        else:
            print(f"  ✅ パース成功（{len(result['items'])} 項目）")
        
        print()
    
    print("=" * 70)
    print("✅ テスト完了")
    print("=" * 70)


if __name__ == "__main__":
    # 統合パーサーのテスト
    test_universal_parser()
    
    print()
    print("=" * 70)
    print("次のステップ:")
    print("  1. batch_process() でバッチ処理をテスト（10ファイル）")
    print("  2. BigQuery投入のテスト")
    print("  3. 全件処理（179ファイル）")
    print("=" * 70)