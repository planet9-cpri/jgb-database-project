"""
Phase 5 統合パーサー v8: 改善版（全指摘事項対応）

改善内容:
  1. 認証とデータセットをコンストラクタ引数化
  2. 和数字の過剰置換を修正（複雑な組み合わせは除外）
  3. 正規化順序を修正（丸数字→NFKC）
  4. 償還金額の正規表現を修正（につき）
  5. UTC時刻の使用とトランザクション整合性
  6. 複数銘柄名の抽出を強化
  7. 法令名正規化の汎用化
  8. 別表の総額チェック
  9. ゼロ除算の回避
"""

import re
import json
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
import os
from google.cloud import bigquery


# =============================================================================
# 正規化基盤（改善版）
# =============================================================================

# 丸数字の対応表
# 重要: これらはUnicode正規化（NFKC）の前に処理する必要があります
# NFKCを先に実行すると、①が1に変換されてしまい、この置換が効かなくなります
CIRCLED_NUMBERS = {
    '⑴': '(1)', '⑵': '(2)', '⑶': '(3)', '⑷': '(4)', '⑸': '(5)',
    '①': '(1)', '②': '(2)', '③': '(3)', '④': '(4)', '⑤': '(5)',
    '⓵': '(1)', '⓶': '(2)', '⓷': '(3)', '⓸': '(4)', '⓹': '(5)',
    '（１）': '(1)', '（２）': '(2)', '（３）': '(3)', '（４）': '(4)', '（５）': '(5)',
    '(１)': '(1)', '(２)': '(2)', '(３)': '(3)', '(４)': '(4)', '(５)': '(5)',
}

# 漢数字の対応表（安全な単独置換のみ）
# 注意: 「十一」や「百二十三」のような複雑な組み合わせは扱いません
# これらを単純置換すると「十一」→「101」のような誤変換が発生します
# 実際の告示では年月日は既に算用数字で書かれていることが多いため、
# ここでは最も基本的なケースのみを対象とします
SAFE_KANJI_NUMBERS = {
    '〇': '0',
    '零': '0',
    '元': '1',  # 「令和元年」→「令和1年」のため
}


def normalize_text(text: str) -> str:
    """
    告示テキストを標準形式に正規化します
    
    処理順序が重要です:
    1. 丸数字の置換（NFKCの前に実行）
    2. Unicode正規化（NFKC）
    3. 安全な漢数字の置換
    4. 空白文字の統一
    
    なぜこの順序なのか:
    ①をNFKCで処理すると1に変換されてしまい、(1)に置き換える機会を失います
    そのため、丸数字の置換を最初に行います
    
    Args:
        text: 元の告示テキスト
    
    Returns:
        正規化されたテキスト
    """
    # ステップ1: 丸数字の置換（最優先）
    # NFKCより前に実行することで、①が1に化ける前に(1)に変換できます
    for original, normalized in CIRCLED_NUMBERS.items():
        text = text.replace(original, normalized)
    
    # ステップ2: Unicode正規化（NFKC）
    # 全角文字を半角に、互換文字を標準形式に統一します
    # 例: 「１２３」→「123」、「％」→「%」
    text = unicodedata.normalize('NFKC', text)
    
    # ステップ3: 安全な漢数字の置換
    # 複雑な組み合わせ（十一、百二十三など）は対象外とし、
    # 単独で安全に置き換えられるもののみを処理します
    for kanji, digit in SAFE_KANJI_NUMBERS.items():
        text = text.replace(kanji, digit)
    
    # ステップ4: 空白文字の統一
    # 全角スペース、タブ、連続する空白を単一の半角スペースに統一します
    # ただし、改行文字は文書構造を保持するため残します
    text = re.sub(r'[ \t\u3000]+', ' ', text)
    
    return text


def parse_japanese_date(text: str) -> Optional[str]:
    """
    和暦を西暦に変換します
    
    対応する元号: 令和、平成、昭和
    
    Args:
        text: 日付を含むテキスト（例: 「令和5年10月11日」）
    
    Returns:
        ISO形式の日付文字列（例: '2023-10-11'）
        解析できない場合はNone
    """
    ERA_BASE_YEARS = {
        '令和': 2018,
        '平成': 1988,
        '昭和': 1925
    }
    
    match = re.search(r'(令和|平成|昭和)(\d+)年(\d+)月(\d+)日', text)
    if not match:
        return None
    
    era = match.group(1)
    year = int(match.group(2)) + ERA_BASE_YEARS[era]
    month = int(match.group(3))
    day = int(match.group(4))
    
    return f'{year}-{month:02d}-{day:02d}'


def safe_extract_amount(text: str) -> Optional[int]:
    """
    金額を安全に抽出します
    
    複数のパターンを順番に試すことで、表記ゆれに対応します
    より具体的なパターンから試し、最後に最も緩いパターンを試します
    
    Args:
        text: 金額を含むテキスト
    
    Returns:
        抽出された金額（整数）
        抽出できない場合はNone
    """
    patterns = [
        r'額面金額で([\d,]+)円',      # 標準形式
        r'額面金額([\d,]+)円',         # 「で」省略
        r'金額で([\d,]+)円',           # 「額面」省略
        r'金額([\d,]+)円',             # 「額面」と「で」の両方省略
        r'([\d,]+)円',                 # 最もシンプルな形式（最後の手段）
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            try:
                amount_str = match.group(1).replace(',', '')
                return int(amount_str)
            except (ValueError, AttributeError):
                continue  # このパターンは失敗、次を試す
    
    return None


def extract_law_reference(law_ref: str) -> Optional[Tuple[str, str, str]]:
    """
    法令参照から法律名、条、項を汎用的に抽出します
    
    この関数は、「財政法第4条第1項」のような文字列から、
    法律名（財政法）、条（4）、項（1）を個別に抽出します
    
    従来の実装では「財政法なら第4条固定」としていましたが、
    これでは将来的に他の条項が出現した際に対応できません
    この汎用的な実装により、どんな条項にも対応できます
    
    Args:
        law_ref: 法令参照文字列（例: 「財政法第4条第1項」）
    
    Returns:
        (法律名, 条番号, 項番号) のタプル
        解析できない場合はNone
    """
    # パターン: 法律名 + 第N条第M項
    match = re.search(r'(.+?)第(\d+)条第(\d+)項', law_ref)
    if match:
        law_name = match.group(1).strip()
        article = match.group(2)
        paragraph = match.group(3)
        return (law_name, article, paragraph)
    
    return None


def normalize_law_key(law_name: str, article: str, paragraph: str) -> str:
    """
    法令キーを標準形式に正規化します
    
    法律名の表記ゆれ（略称など）を統一し、
    一貫したキー形式を生成します
    
    Args:
        law_name: 法律名（略称の可能性あり）
        article: 条番号
        paragraph: 項番号
    
    Returns:
        正規化された法令キー（例: '財政法第4条第1項'）
    """
    # 法律名の正規化（略称→正式名称）
    if '財政法' in law_name and '財政運営' not in law_name:
        normalized_name = '財政法'
    elif '財政運営' in law_name:
        normalized_name = '財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律'
    elif '特別会計' in law_name or '特会' in law_name:
        normalized_name = '特別会計に関する法律'
    else:
        normalized_name = law_name
    
    return f'{normalized_name}第{article}条第{paragraph}項'


# =============================================================================
# NumberedListParser (改善版)
# =============================================================================

class NumberedListParser:
    """番号付きリスト形式のパーサー（改善版）"""
    
    def can_parse(self, text: str) -> float:
        """処理可能性スコア"""
        score = 0.0
        
        # 国庫短期証券の場合はスコアを大幅に下げる
        if '国庫短期証券' in text or '割引短期国債' in text or '政府短期証券' in text:
            return 0.1
        
        has_item1 = bool(re.search(r'\b1\s+名称及び記号', text))
        has_item2 = bool(re.search(r'\b2\s+発行の根拠法律', text))
        has_item_amount = bool(re.search(r'\b[56]\s+.*?発.*?行.*?額', text))
        
        if has_item1 and has_item2 and has_item_amount:
            score += 0.4
        
        if '価格競争入札' in text and '非価格競争入札' in text:
            score += 0.3
        
        if '並びに' in text and '及び' in text:
            score += 0.2
        
        if '(別表のとおり)' not in text and '別表' not in text:
            score += 0.3
        
        return min(score, 1.0)
    
    def extract(self, text: str) -> Dict[str, Any]:
        """データ抽出"""
        items = {}
        
        # 項目1: 名称及び記号
        match = re.search(r'\b1\s+名称及び記号\s+(.+?)(?=\n2\b)', text, re.DOTALL)
        if match:
            items[1] = {
                'title': '名称及び記号',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_item1(match.group(1))
            }
        
        # 項目2: 発行の根拠法律
        match = re.search(r'\b2\s+発行の根拠法律及びその条項\s+(.+?)(?=\n3\b)', text, re.DOTALL)
        if match:
            items[2] = {
                'title': '発行の根拠法律及びその条項',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_item2(match.group(1))
            }
        
        # 項目6: 発行額
        match = re.search(r'\b6\s+発行額(.+?)(?=\n7\b)', text, re.DOTALL)
        if match:
            full_text = match.group(1).strip()
            items[6] = {
                'title': '発行額',
                'value': full_text,
                'sub_number': None,
                'structured_data': self._parse_item6(full_text)
            }
            
            # サブ項目も抽出
            sub_items = self._extract_sub_items(full_text)
            for sub_num, sub_data in sub_items.items():
                items[f'6_{sub_num}'] = {
                    'title': '発行額サブ項目',
                    'value': sub_data['value'],
                    'sub_number': sub_num,
                    'structured_data': sub_data['structured']
                }
        
        # 項目10: 発行日
        match = re.search(r'\b10\s+発行日\s+(.+?)(?=\n11\b)', text, re.DOTALL)
        if match:
            items[10] = {
                'title': '発行日',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': {'issue_date': parse_japanese_date(match.group(1))}
            }
        
        # 項目12: 利率
        match = re.search(r'\b12\s+利率\s+(.+?)(?=\n13\b)', text, re.DOTALL)
        if match:
            items[12] = {
                'title': '利率',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_rate(match.group(1))
            }
        
        # 項目16: 償還期限
        match = re.search(r'\b16\s+償還期限\s+(.+?)(?=\n17\b)', text, re.DOTALL)
        if match:
            items[16] = {
                'title': '償還期限',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': {'maturity_date': parse_japanese_date(match.group(1))}
            }
        
        # 項目17: 償還金額
        match = re.search(r'\b17\s+償還金額\s+(.+?)(?=\n18\b)', text, re.DOTALL)
        if match:
            items[17] = {
                'title': '償還金額',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_redemption_amount(match.group(1))
            }
        
        return {'pattern': 'NUMBERED_LIST_MULTI_LAW', 'items': items}
    
    def _extract_sub_items(self, text: str) -> dict:
        """(1)(2)(3)のサブ項目を抽出"""
        sub_items = {}
        
        match1 = re.search(r'\(1\)\s*価格競争入札発行(.+?)(?=\(2\)|$)', text, re.DOTALL)
        if match1:
            sub_items[1] = {
                'value': f'(1) 価格競争入札発行{match1.group(1).strip()}',
                'structured': self._parse_section_with_context(match1.group(1))
            }
        
        match2 = re.search(r'\(2\)\s*国債市場特別参加者.*?第I非価格競争入札発行(.+?)(?=\(3\)|$)', text, re.DOTALL)
        if match2:
            sub_items[2] = {
                'value': f'(2) 第I非価格競争入札発行{match2.group(1).strip()}',
                'structured': self._parse_section_with_context(match2.group(1))
            }
        
        match3 = re.search(r'\(3\)\s*国債市場特別参加者.*?第II非価格競争入札発行(.+?)$', text, re.DOTALL)
        if match3:
            sub_items[3] = {
                'value': f'(3) 第II非価格競争入札発行{match3.group(1).strip()}',
                'structured': self._parse_section_with_context(match3.group(1))
            }
        
        return sub_items
    
    def _parse_section_with_context(self, section_text: str) -> dict:
        """
        セクションを独立したコンテキストで解析
        
        重要: last_law_nameはこのメソッドのローカル変数です
        これにより、セクションをまたいで誤った法律名が継承されることを防ぎます
        """
        result = {'total_amount': 0, 'by_law': {}}
        last_law_name = None  # このセクション専用のコンテキスト
        
        # 総額を抽出
        amount = safe_extract_amount(section_text)
        if amount:
            result['total_amount'] = amount
        
        # 明示的な法令名のマッチ
        law_matches = re.finditer(
            r'([^、]+第\d+条第\d+項)の規定に基づ.*?額面金額(?:で)?([\d,]+)円',
            section_text
        )
        
        for match in law_matches:
            law_ref = match.group(1).strip()
            amount = int(match.group(2).replace(',', ''))
            
            # 汎用的な法令抽出を使用
            parsed = extract_law_reference(law_ref)
            if parsed:
                law_name, article, paragraph = parsed
                law_key = normalize_law_key(law_name, article, paragraph)
                result['by_law'][law_key] = amount
                
                # このセクション内での現在の法律を記憶
                last_law_name = law_name
        
        # 「同法」参照を処理
        same_law_matches = re.finditer(
            r'同法第(\d+)条第(\d+)項の規定に基づ.*?額面金額(?:で)?([\d,]+)円',
            section_text
        )
        
        for match in same_law_matches:
            article = match.group(1)
            paragraph = match.group(2)
            amount = int(match.group(3).replace(',', ''))
            
            if last_law_name:
                # last_law_nameを使って完全な法令キーを生成
                law_key = normalize_law_key(last_law_name, article, paragraph)
                result['by_law'][law_key] = amount
            else:
                # 警告: 参照先が不明
                # データは破棄せず、不明な法令として記録
                print(f"⚠️ 警告: '同法'参照があるが参照先が不明")
                print(f"   第{article}条第{paragraph}項、金額: {amount:,}円")
                law_key = f'不明な法令_第{article}条第{paragraph}項'
                result['by_law'][law_key] = amount
        
        return result
    
    def _parse_item1(self, text: str) -> dict:
        """項目1: 名称及び記号"""
        match = re.search(r'利\s*付国庫債券\((.+?)\)\(第(\d+)回\)', text)
        if match:
            return {
                'bond_name': f'利付国庫債券({match.group(1)})',
                'bond_series': f'第{match.group(2)}回'
            }
        return {'raw': text}
    
    def _parse_item2(self, text: str) -> dict:
        """項目2: 発行の根拠法律"""
        laws = []
        
        # 汎用的な法令抽出を使用
        # 複数の法令が記載されている場合に対応
        law_matches = re.finditer(r'([^、。]+第\d+条第\d+項)', text)
        
        for match in law_matches:
            law_ref = match.group(1).strip()
            parsed = extract_law_reference(law_ref)
            if parsed:
                law_name, article, paragraph = parsed
                law_key = normalize_law_key(law_name, article, paragraph)
                
                # 法律の正式名称を取得
                if '財政法' in law_name and '財政運営' not in law_name:
                    full_name = '財政法(昭和22年法律第34号)'
                    short_name = '財政法'
                elif '財政運営' in law_name:
                    full_name = '財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律(平成24年法律第101号)'
                    short_name = '財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律'
                elif '特別会計' in law_name or '特会' in law_name:
                    full_name = '特別会計に関する法律(平成19年法律第23号)'
                    short_name = '特別会計に関する法律'
                else:
                    full_name = law_name
                    short_name = law_name
                
                laws.append({
                    'name': short_name,
                    'full_name': full_name,
                    'article': f'第{article}条第{paragraph}項',
                    'key': law_key,
                    'amount': 0
                })
        
        return {'laws': laws}
    
    def _parse_item6(self, text: str) -> dict:
        """項目6: 発行額"""
        result = {
            'total_amount': 0,
            'competitive': {'amount': 0, 'by_law': {}},
            'noncompetitive1': {'amount': 0, 'by_law': {}},
            'noncompetitive2': {'amount': 0, 'by_law': {}}
        }
        
        # 各セクションを独立して処理
        sections = [
            ('competitive', r'\(1\)(.+?)(?=\(2\)|$)'),
            ('noncompetitive1', r'\(2\)(.+?)(?=\(3\)|$)'),
            ('noncompetitive2', r'\(3\)(.+?)$')
        ]
        
        for section_name, pattern in sections:
            match = re.search(pattern, text, re.DOTALL)
            if match:
                parsed = self._parse_section_with_context(match.group(1))
                result[section_name] = parsed
                result['total_amount'] += parsed['total_amount']
        
        return result
    
    def _parse_rate(self, text: str) -> dict:
        """利率を解析"""
        match = re.search(r'年\s*([\d.]+)\s*%', text)
        if match:
            return {'rate': float(match.group(1))}
        return {'raw': text}
    
    def _parse_redemption_amount(self, text: str) -> dict:
        """
        償還金額を解析
        
        修正: 「につ」ではなく「につき」が正しい表現です
        """
        match = re.search(r'額面金額100円につき(\d+)円', text)
        if match:
            return {'amount': int(match.group(1))}
        return {'raw': text}


# =============================================================================
# TableParserV4 (改善版)
# =============================================================================

class TableParserV4:
    """横並び別表形式のパーサー（改善版）"""
    
    def can_parse(self, text: str) -> float:
        """処理可能性スコア"""
        score = 0.0
        
        if '(別表のとおり)' in text or '内訳(別表のとおり)' in text:
            score += 0.4
        
        if '名称及び記号' in text and '利率' in text and '償還期限' in text:
            score += 0.4
        
        if '(別表)' in text:
            score += 0.2
        
        return min(score, 1.0)
    
    def extract(self, text: str) -> Dict[str, Any]:
        """データ抽出"""
        items = {}
        
        # 項目1: 名称及び記号
        match = re.search(r'\b1\s+名称及び記号\s+(.+?)(?=\n2\b)', text, re.DOTALL)
        if match:
            bond_names = self._parse_multiple_bond_names(match.group(1))
            items[1] = {
                'title': '名称及び記号',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': {'bond_names': bond_names}
            }
        
        # 項目2: 発行の根拠法律
        match = re.search(r'\b2\s+発行の根拠法律及びその条項\s+(.+?)(?=\n3\b)', text, re.DOTALL)
        if match:
            items[2] = {
                'title': '発行の根拠法律及びその条項',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_laws(match.group(1))
            }
        
        # 項目6: 発行額（別表参照）
        match = re.search(r'\b6\s+発行額\s+額面金額(?:で)?([\d,]+)円', text, re.DOTALL)
        if match:
            total_amount = int(match.group(1).replace(',', ''))
            items[6] = {
                'title': '発行額',
                'value': f'額面金額で{match.group(1)}円\n内訳(別表のとおり)',
                'sub_number': None,
                'structured_data': {'total_amount': total_amount}
            }
        
        # 項目10: 発行日
        match = re.search(r'\b10\s+発行日\s+(.+?)(?=\n11\b)', text, re.DOTALL)
        if match:
            items[10] = {
                'title': '発行日',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': {'issue_date': parse_japanese_date(match.group(1))}
            }
        
        # 別表の解析
        table_data = self._parse_table_robust(text)
        if table_data:
            # 整合性チェック: 別表の合計と項目6の総額を比較
            table_sum = sum(row['issue_amount'] for row in table_data)
            if 6 in items:
                declared_total = items[6]['structured_data']['total_amount']
                
                # ±1円までの誤差は許容（端数処理の違いなど）
                if abs(table_sum - declared_total) > 1:
                    print(f"⚠️ 警告: 別表の合計額と項目6の総額が一致しません")
                    print(f"   項目6の総額: {declared_total:,}円")
                    print(f"   別表の合計: {table_sum:,}円")
                    print(f"   差額: {abs(table_sum - declared_total):,}円")
            
            # 別表の各行を項目6のサブ項目として追加
            for i, row in enumerate(table_data, start=1):
                items[f'6_{i}'] = {
                    'title': '発行額(別表)',
                    'value': self._format_table_row(row),
                    'sub_number': i,
                    'structured_data': row
                }
        
        return {'pattern': 'TABLE_HORIZONTAL', 'items': items}
    
    def _parse_table_robust(self, text: str) -> list:
        """
        表を堅牢に解析
        
        改善点:
        - 「分」の有無に対応（(?:分)?）
        - 元号の選択（令和|平成）
        - スペースの柔軟性
        """
        table_start = text.find('(別表)')
        if table_start == -1:
            return []
        
        table_text = text[table_start:]
        rows = []
        
        # 改善された正規表現パターン
        pattern = (
            r'利付国庫債券\((.+?)\)\s*'
            r'\(第(\d+)回\)\s+'
            r'([\d.]+)%\s+'
            r'(?:令和|平成)(\d+)年(\d+)月(\d+)日\s+'
            r'特別会計に関する法律第(\d+)条第1項(?:分)?\s+'
            r'([\d,]+)円'
        )
        
        matches = re.finditer(pattern, table_text)
        
        for match in matches:
            bond_type = match.group(1)
            series = match.group(2)
            rate = float(match.group(3))
            
            # 元号判定（マッチした部分全体から判定）
            if '令和' in match.group(0):
                year = int(match.group(4)) + 2018
            else:  # 平成
                year = int(match.group(4)) + 1988
            
            month = int(match.group(5))
            day = int(match.group(6))
            law_article = match.group(7)
            amount = int(match.group(8).replace(',', ''))
            
            law_key = f'特別会計に関する法律第{law_article}条第1項'
            
            row = {
                'bond_name': f'利付国庫債券({bond_type})',
                'bond_series': f'第{series}回',
                'interest_rate': rate,
                'maturity_date': f'{year}-{month:02d}-{day:02d}',
                'law_key': law_key,
                'law_article': f'第{law_article}条第1項',
                'issue_amount': amount
            }
            
            rows.append(row)
        
        return rows
    
    def _parse_multiple_bond_names(self, text: str) -> list:
        """
        複数の銘柄名を抽出（改善版）
        
        改善点: 「及び」だけでなく「並びに」や読点でも分割
        """
        bond_names = []
        
        # 複数の区切り文字で分割
        # 「及び」「並びに」「、」のいずれかで分割
        parts = re.split(r'及び|並びに|、', text)
        
        for part in parts:
            part = part.strip()
            if '利付国庫債券' in part:
                bond_names.append(part)
        
        return bond_names
    
    def _parse_laws(self, text: str) -> dict:
        """法令を抽出"""
        laws = []
        
        # 汎用的な法令抽出を使用
        law_matches = re.finditer(r'([^、。]+第\d+条第\d+項)', text)
        
        for match in law_matches:
            law_ref = match.group(1).strip()
            parsed = extract_law_reference(law_ref)
            if parsed:
                law_name, article, paragraph = parsed
                law_key = normalize_law_key(law_name, article, paragraph)
                
                if '特別会計' in law_name or '特会' in law_name:
                    full_name = '特別会計に関する法律(平成19年法律第23号)'
                    short_name = '特別会計に関する法律'
                else:
                    full_name = law_name
                    short_name = law_name
                
                laws.append({
                    'name': short_name,
                    'full_name': full_name,
                    'article': f'第{article}条第{paragraph}項',
                    'key': law_key,
                    'amount': 0
                })
        
        return {'laws': laws}
    
    def _format_table_row(self, row: dict) -> str:
        """表の行を文字列にフォーマット"""
        return (f"{row['bond_name']}{row['bond_series']}: "
                f"利率{row['interest_rate']}%, "
                f"償還期限{row['maturity_date']}, "
                f"{row['law_key']}, "
                f"発行額{row['issue_amount']:,}円")


# =============================================================================
# RetailBondParser (改善版)
# =============================================================================

class RetailBondParser:
    """個人向け国債のパーサー（改善版）"""
    
    def can_parse(self, text: str) -> float:
        """処理可能性スコア"""
        if '個人向け利付国庫債券' in text or '個人向け国債' in text:
            return 1.0
        return 0.0
    
    def extract(self, text: str) -> Dict[str, Any]:
        """データ抽出"""
        items = {}
        
        # 項目1: 名称及び記号
        match = re.search(r'\b1\s+名称及び記号\s+(.+?)(?=\n2\b)', text, re.DOTALL)
        if match:
            items[1] = {
                'title': '名称及び記号',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_item1(match.group(1))
            }
        
        # 項目2: 発行の根拠法律
        match = re.search(r'\b2\s+発行の根拠法律及びその条項\s+(.+?)(?=\n3\b)', text, re.DOTALL)
        if match:
            items[2] = {
                'title': '発行の根拠法律及びその条項',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_laws(match.group(1))
            }
        
        # 項目4: 発行額（個人向けは項目4）
        match = re.search(r'\b4\s+発行額\s+額面金額(?:で)?([\d,]+)円', text, re.DOTALL)
        if match:
            total_amount = int(match.group(1).replace(',', ''))
            items[4] = {
                'title': '発行額',
                'value': f'額面金額で{match.group(1)}円',
                'sub_number': None,
                'structured_data': {'total_amount': total_amount}
            }
        
        # 項目7: 発行日（個人向けは項目7）
        match = re.search(r'\b7\s+発行日\s+(.+?)(?=\n8\b)', text, re.DOTALL)
        if match:
            items[7] = {
                'title': '発行日',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': {'issue_date': parse_japanese_date(match.group(1))}
            }
        
        # 項目9: 初期利子の適用利率
        match = re.search(r'\b9\s+初期利子の適用利率\s+年\s*([\d.]+)\s*%', text, re.DOTALL)
        if match:
            items[9] = {
                'title': '初期利子の適用利率',
                'value': f'年{match.group(1)}%',
                'sub_number': None,
                'structured_data': {'rate': float(match.group(1))}
            }
        
        # 項目13: 償還期限
        match = re.search(r'\b13\s+償還期限\s+(.+?)(?=\n14\b)', text, re.DOTALL)
        if match:
            items[13] = {
                'title': '償還期限',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': {'maturity_date': parse_japanese_date(match.group(1))}
            }
        
        # 項目14: 償還金額（修正: につき）
        match = re.search(r'\b14\s+償還金額\s+(.+?)(?=\n15\b)', text, re.DOTALL)
        if match:
            items[14] = {
                'title': '償還金額',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_redemption_amount(match.group(1))
            }
        
        return {'pattern': 'RETAIL_BOND', 'items': items}
    
    def _parse_item1(self, text: str) -> dict:
        """項目1: 名称及び記号"""
        match = re.search(r'個人向け利付国庫債券\((.+?)\)\(第(\d+)回\)', text)
        if match:
            return {
                'bond_name': f'個人向け利付国庫債券({match.group(1)})',
                'bond_series': f'第{match.group(2)}回',
                'bond_type': match.group(1)
            }
        return {'raw': text}
    
    def _parse_laws(self, text: str) -> dict:
        """法令を抽出"""
        laws = []
        
        law_matches = re.finditer(r'([^、。]+第\d+条第\d+項)', text)
        
        for match in law_matches:
            law_ref = match.group(1).strip()
            parsed = extract_law_reference(law_ref)
            if parsed:
                law_name, article, paragraph = parsed
                law_key = normalize_law_key(law_name, article, paragraph)
                
                if '特別会計' in law_name or '特会' in law_name:
                    full_name = '特別会計に関する法律(平成19年法律第23号)'
                    short_name = '特別会計に関する法律'
                else:
                    full_name = law_name
                    short_name = law_name
                
                laws.append({
                    'name': short_name,
                    'full_name': full_name,
                    'article': f'第{article}条第{paragraph}項',
                    'key': law_key,
                    'amount': 0
                })
        
        return {'laws': laws}
    
    def _parse_redemption_amount(self, text: str) -> dict:
        """償還金額を解析（修正: につき）"""
        match = re.search(r'額面金額100円につき(\d+)円', text)
        if match:
            return {'amount': int(match.group(1))}
        return {'raw': text}


# =============================================================================
# TBParser (改善版)
# =============================================================================

class TBParser:
    """国庫短期証券のパーサー（改善版）"""
    
    def can_parse(self, text: str) -> float:
        """処理可能性スコア"""
        score = 0.0
        
        if '国庫短期証券' in text:
            score += 0.5
        
        if '割引短期国債' in text or '政府短期証券' in text:
            score += 0.5
        
        return min(score, 1.0)
    
    def extract(self, text: str) -> Dict[str, Any]:
        """データ抽出"""
        items = {}
        
        # 項目1: 名称及び記号
        match = re.search(r'\b1\s+名称及び記号\s+(.+?)(?=\n2\b)', text, re.DOTALL)
        if match:
            items[1] = {
                'title': '名称及び記号',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_item1(match.group(1))
            }
        
        # 項目2: 発行の根拠法律
        match = re.search(r'\b2\s+発行の根拠法律及びその条項\s+(.+?)(?=\n3\b)', text, re.DOTALL)
        if match:
            items[2] = {
                'title': '発行の根拠法律及びその条項',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_laws(match.group(1))
            }
        
        # 項目6: 発行額
        match = re.search(r'\b6\s+発行額(.+?)(?=\n7\b)', text, re.DOTALL)
        if match:
            full_text = match.group(1).strip()
            items[6] = {
                'title': '発行額',
                'value': full_text,
                'sub_number': None,
                'structured_data': self._parse_item6(full_text)
            }
            
            # サブ項目も抽出
            sub_items = self._extract_sub_items(full_text)
            for sub_num, sub_data in sub_items.items():
                items[f'6_{sub_num}'] = {
                    'title': '発行額サブ項目',
                    'value': sub_data['value'],
                    'sub_number': sub_num,
                    'structured_data': sub_data['structured']
                }
        
        # 項目10: 発行日
        match = re.search(r'\b10\s+発行日\s+(.+?)(?=\n11\b)', text, re.DOTALL)
        if match:
            items[10] = {
                'title': '発行日',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': {'issue_date': parse_japanese_date(match.group(1))}
            }
        
        # 項目12: 償還期限
        match = re.search(r'\b12\s+償還期限\s+(.+?)(?=\n13\b)', text, re.DOTALL)
        if match:
            items[12] = {
                'title': '償還期限',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': {'maturity_date': parse_japanese_date(match.group(1))}
            }
        
        # 項目13: 償還金額（修正: につき）
        match = re.search(r'\b13\s+償還金額\s+(.+?)(?=\n14\b)', text, re.DOTALL)
        if match:
            items[13] = {
                'title': '償還金額',
                'value': match.group(1).strip(),
                'sub_number': None,
                'structured_data': self._parse_redemption_amount(match.group(1))
            }
        
        return {'pattern': 'TB_SHORT_TERM', 'items': items}
    
    def _extract_sub_items(self, text: str) -> dict:
        """(1)(2)のサブ項目を抽出"""
        sub_items = {}
        
        match1 = re.search(r'\(1\)\s*価格競争入札発行(.+?)(?=\(2\)|$)', text, re.DOTALL)
        if match1:
            sub_items[1] = {
                'value': f'(1) 価格競争入札発行{match1.group(1).strip()}',
                'structured': self._parse_section_with_context(match1.group(1))
            }
        
        match2 = re.search(r'\(2\)\s*国債市場特別参加者.*?第I非価格競争入札発行(.+?)$', text, re.DOTALL)
        if match2:
            sub_items[2] = {
                'value': f'(2) 第I非価格競争入札発行{match2.group(1).strip()}',
                'structured': self._parse_section_with_context(match2.group(1))
            }
        
        return sub_items
    
    def _parse_section_with_context(self, section_text: str) -> dict:
        """セクションを独立したコンテキストで解析"""
        result = {'total_amount': 0, 'by_law': {}}
        
        # 総額を抽出
        amount = safe_extract_amount(section_text)
        if amount:
            result['total_amount'] = amount
        
        # 割引短期国債
        tb_match = re.search(
            r'特別会計に関する法律第46条第1項.*?割引短期国債.*?額面金額(?:で)?([\d,]+)円',
            section_text,
            re.DOTALL
        )
        if tb_match:
            amount = int(tb_match.group(1).replace(',', ''))
            result['by_law']['特別会計に関する法律第46条第1項'] = amount
        
        # 政府短期証券
        gsb_match = re.search(
            r'財政法第7条第1項.*?政府短期証券.*?額面金額(?:で)?([\d,]+)円',
            section_text,
            re.DOTALL
        )
        if gsb_match:
            amount = int(gsb_match.group(1).replace(',', ''))
            result['by_law']['政府短期証券関連法令'] = amount
        
        return result
    
    def _parse_item1(self, text: str) -> dict:
        """項目1: 名称及び記号"""
        match = re.search(r'国庫短期証券\(第(\d+)回\)', text)
        if match:
            return {
                'bond_name': '国庫短期証券',
                'bond_series': f'第{match.group(1)}回'
            }
        return {'raw': text}
    
    def _parse_laws(self, text: str) -> dict:
        """法令を抽出"""
        laws = []
        
        # 汎用的な法令抽出
        law_matches = re.finditer(r'([^、。]+第\d+条第\d+項)', text)
        
        for match in law_matches:
            law_ref = match.group(1).strip()
            parsed = extract_law_reference(law_ref)
            if parsed:
                law_name, article, paragraph = parsed
                law_key = normalize_law_key(law_name, article, paragraph)
                
                if '特別会計' in law_name or '特会' in law_name:
                    full_name = '特別会計に関する法律(平成19年法律第23号)'
                    short_name = '特別会計に関する法律'
                else:
                    full_name = law_name
                    short_name = law_name
                
                laws.append({
                    'name': short_name,
                    'full_name': full_name,
                    'article': f'第{article}条第{paragraph}項',
                    'key': law_key,
                    'amount': 0
                })
        
        # 政府短期証券用の複数条項
        tb_articles = ['83', '94', '95', '136', '137']
        if any(f'第{a}条' in text for a in tb_articles):
            laws.append({
                'name': '特別会計に関する法律(政府短期証券)',
                'full_name': '特別会計に関する法律(複数条項)',
                'article': '第83条等',
                'key': '政府短期証券関連法令',
                'amount': 0
            })
        
        return {'laws': laws}
    
    def _parse_item6(self, text: str) -> dict:
        """項目6: 発行額"""
        result = {
            'total_amount': 0,
            'competitive': {'amount': 0, 'by_law': {}},
            'noncompetitive1': {'amount': 0, 'by_law': {}}
        }
        
        # (1) 価格競争入札
        comp_match = re.search(r'\(1\)\s*価格競争入札発行\s*額面金額(?:で)?([\d,]+)円', text)
        if comp_match:
            amount = int(comp_match.group(1).replace(',', ''))
            result['competitive']['amount'] = amount
            result['total_amount'] += amount
            
            comp_section = re.search(r'\(1\)(.+?)(?=\(2\)|$)', text, re.DOTALL)
            if comp_section:
                result['competitive']['by_law'] = self._parse_section_with_context(comp_section.group(1))['by_law']
        
        # (2) 第I非価格競争
        nc1_match = re.search(r'\(2\).*?額面金額(?:で)?([\d,]+)円', text, re.DOTALL)
        if nc1_match:
            amount = int(nc1_match.group(1).replace(',', ''))
            result['noncompetitive1']['amount'] = amount
            result['total_amount'] += amount
            
            nc1_section = re.search(r'\(2\)(.+?)$', text, re.DOTALL)
            if nc1_section:
                result['noncompetitive1']['by_law'] = self._parse_section_with_context(nc1_section.group(1))['by_law']
        
        return result
    
    def _parse_redemption_amount(self, text: str) -> dict:
        """償還金額を解析（修正: につき）"""
        match = re.search(r'額面金額100円につき(\d+)円', text)
        if match:
            return {'amount': int(match.group(1))}
        return {'raw': text}


# =============================================================================
# UniversalAnnouncementParser (改善版)
# =============================================================================

class UniversalAnnouncementParser:
    """統合パーサー v8: 改善版"""
    
    def __init__(self, project_id: str = None, dataset_id: str = None, bq_client: bigquery.Client = None):
        """
        コンストラクタ
        
        改善: 認証情報とデータセットIDをハードコードせず、
        引数として受け取るか環境変数から取得します
        
        Args:
            project_id: BigQuery プロジェクトID（Noneの場合は環境変数から取得）
            dataset_id: BigQuery データセットID（Noneの場合は環境変数から取得）
            bq_client: BigQuery クライアント（Noneの場合は新規作成）
        """
        self.parsers = [
            ('RETAIL_BOND', RetailBondParser()),
            ('TABLE_HORIZONTAL', TableParserV4()),
            ('TB_SHORT_TERM', TBParser()),
            ('NUMBERED_LIST_MULTI_LAW', NumberedListParser()),
        ]
        
        # プロジェクトIDとデータセットIDの決定
        self.project_id = project_id or os.environ.get('GCP_PROJECT_ID', 'jgb2023')
        self.dataset_id = dataset_id or os.environ.get('BQ_DATASET_ID', '20251028')
        
        # BigQueryクライアントの初期化
        if bq_client:
            self.client = bq_client
        else:
            # 環境変数から認証情報のパスを取得
            # 設定されていない場合は、Application Default Credentials (ADC) を使用
            cred_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
            if cred_path:
                self.client = bigquery.Client(project=self.project_id)
            else:
                # ADCを使用（本番環境推奨）
                self.client = bigquery.Client(project=self.project_id)
    
    def parse(self, text: str, file_path: Optional[Path] = None) -> Dict[str, Any]:
        """告示を解析"""
        try:
            # テキストを正規化
            normalized_text = normalize_text(text)
            
            # パターン識別
            pattern, confidence = self.identify_pattern(normalized_text)
            
            if confidence < 0.5:
                return {
                    'pattern': 'UNKNOWN',
                    'confidence': confidence,
                    'items': {},
                    'error': f'パターン識別の信頼度が低い({confidence:.2f})',
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
            result = parser.extract(normalized_text)
            
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
    
    def identify_pattern(self, text: str) -> Tuple[str, float]:
        """パターンを自動識別"""
        scores = {}
        
        for pattern_name, parser in self.parsers:
            score = parser.can_parse(text)
            scores[pattern_name] = score
        
        best_pattern = max(scores.items(), key=lambda x: x[1])
        return best_pattern[0], best_pattern[1]
    
    def extract_metadata_from_filename(self, file_path: Path) -> Dict[str, str]:
        """ファイル名からメタデータを抽出"""
        filename = file_path.stem
        
        match = re.match(r'(\d{8})_令和(\d+)年(\d+)月(\d+)日付\(財務省(.+?)\)', filename)
        
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
        """
        BigQueryにデータを投入
        
        改善点:
        - UTC時刻を使用
        - Layer2失敗時のロールバック処理
        """
        
        try:
            metadata = self.extract_metadata_from_filename(file_path)
            
            # 発行日を抽出
            issue_date = None
            if 10 in items:
                issue_date = items[10].get('structured_data', {}).get('issue_date')
            elif 7 in items:
                issue_date = items[7].get('structured_data', {}).get('issue_date')
            
            if not issue_date:
                issue_date = metadata['announcement_date']
            
            # 改善: UTC時刻を使用
            now_utc = datetime.now(timezone.utc).isoformat()
            
            # Layer 1: raw_announcements
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
                'parsed_at': now_utc
            }
            
            table_ref = f"{self.project_id}.{self.dataset_id}.raw_announcements"
            errors = self.client.insert_rows_json(table_ref, [raw_row])
            
            if errors:
                print(f"  ✗ Layer 1 投入エラー: {errors}")
                return False
            
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
            
            table_ref = f"{self.project_id}.{self.dataset_id}.announcement_items"
            errors = self.client.insert_rows_json(table_ref, item_rows)
            
            if errors:
                print(f"  ✗ Layer 2 投入エラー: {errors}")
                
                # 改善: Layer2失敗時のロールバック
                # Layer1のparsedフラグをFalseに戻し、エラーメッセージを記録
                try:
                    rollback_query = f"""
                    UPDATE `{self.project_id}.{self.dataset_id}.raw_announcements`
                    SET parsed = FALSE,
                        parse_error = 'Layer2投入失敗: {str(errors)[:200]}'
                    WHERE announcement_id = '{announcement_id}'
                    """
                    self.client.query(rollback_query).result()
                    print(f"  ℹ️ Layer1をロールバックしました")
                except Exception as rollback_error:
                    print(f"  ⚠️ ロールバック失敗: {rollback_error}")
                
                return False
            
            return True
        
        except Exception as e:
            print(f"  ✗ BigQuery投入例外: {e}")
            return False


def batch_process(
    input_dir: Path,
    project_id: str = None,
    dataset_id: str = None,
    test_mode: bool = True,
    max_files: int = 10,
    insert_to_bq: bool = True
) -> Dict[str, Any]:
    """
    バッチ処理: 複数ファイルを一括処理
    
    改善: ゼロ除算の回避
    """
    
    parser = UniversalAnnouncementParser(project_id=project_id, dataset_id=dataset_id)
    files = sorted(input_dir.glob('*.txt'))
    
    if test_mode:
        files = files[:max_files]
        print(f"🧪 テストモード: 最初の{len(files)}ファイルを処理")
    else:
        print(f"🚀 本番モード: {len(files)}ファイルを処理")
    
    print(f"📤 BigQuery投入: {'有効' if insert_to_bq else '無効'}")
    print()
    
    # 改善: ファイルが0件の場合を考慮
    if len(files) == 0:
        print("⚠️ 処理対象のファイルが見つかりませんでした")
        return {
            'total': 0,
            'success': 0,
            'failed': 0,
            'by_pattern': {},
            'errors': [],
            'inserted': 0
        }
    
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
            with open(file_path, 'r', encoding='utf-8') as f:
                text = f.read()
            
            result = parser.parse(text, file_path)
            
            pattern = result['pattern']
            confidence = result['confidence']
            
            print(f"  パターン: {pattern} (信頼度: {confidence:.2f})")
            
            if pattern not in stats['by_pattern']:
                stats['by_pattern'][pattern] = 0
            stats['by_pattern'][pattern] += 1
            
            if result['error']:
                print(f"  ✗ エラー: {result['error']}")
                stats['failed'] += 1
                stats['errors'].append({
                    'file': file_path.name,
                    'pattern': pattern,
                    'error': result['error']
                })
            else:
                print(f"  ✓ パース成功")
                stats['success'] += 1
                
                if insert_to_bq:
                    match = re.match(r'(\d{8})_.*\(財務省(.+?)\)', file_path.name)
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
                        print(f"  ✓ BigQuery投入成功")
                        stats['inserted'] += 1
                    else:
                        print(f"  ✗ BigQuery投入失敗")
            
            print()
        
        except Exception as e:
            print(f"  ✗ 例外: {e}")
            stats['failed'] += 1
            stats['errors'].append({
                'file': file_path.name,
                'pattern': 'EXCEPTION',
                'error': str(e)
            })
            print()
    
    # 結果サマリー（改善: ゼロ除算回避）
    print("=" * 70)
    print("📊 処理結果サマリー")
    print("=" * 70)
    print(f"総ファイル数: {stats['total']}")
    
    if stats['total'] > 0:
        print(f"成功: {stats['success']} ({stats['success']/stats['total']*100:.1f}%)")
        print(f"失敗: {stats['failed']} ({stats['failed']/stats['total']*100:.1f}%)")
        if insert_to_bq:
            print(f"BigQuery投入: {stats['inserted']} ({stats['inserted']/stats['total']*100:.1f}%)")
    else:
        print("成功: 0")
        print("失敗: 0")
    
    print()
    
    if stats['by_pattern']:
        print("パターン別:")
        for pattern, count in sorted(stats['by_pattern'].items(), key=lambda x: x[1], reverse=True):
            if stats['total'] > 0:
                print(f"  {pattern}: {count} ({count/stats['total']*100:.1f}%)")
            else:
                print(f"  {pattern}: {count}")
        print()
    
    if stats['errors']:
        print(f"エラー詳細(最初の5件):")
        for error in stats['errors'][:5]:
            print(f"  - {error['file']}: {error['error'][:50]}...")
        print()
    
    return stats


if __name__ == "__main__":
    print("=" * 80)
    print("🔧 Universal Announcement Parser v8 - 改善版")
    print("=" * 80)
    print()
    print("✅ 全指摘事項対応完了:")
    print("  1. 認証とデータセットをコンストラクタ引数化")
    print("  2. 和数字の過剰置換を修正")
    print("  3. 正規化順序を修正（丸数字→NFKC）")
    print("  4. 償還金額の正規表現を修正（につき）")
    print("  5. UTC時刻の使用とトランザクション整合性")
    print("  6. 複数銘柄名の抽出を強化")
    print("  7. 法令名正規化の汎用化")
    print("  8. 別表の総額チェック")
    print("  9. ゼロ除算の回避")
    print()
    print("=" * 80)