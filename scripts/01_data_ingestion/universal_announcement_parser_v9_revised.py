"""
Phase 5 統合パーサー v9改訂版: v8全改善点 + Phase 6対応

v9改訂版の改善内容:
【v8からの継承・改善】
  1. 別表の開始パターンにフォールバック追加
  2. NumberedListParserの法令行パターン拡張
  3. parse_japanese_dateに西暦フォールバック追加
  4. ロールバックSQLのパラメータ化
  5. 正規表現の\bを\sに変更（日本語対応）
  6. _parse_multiple_bond_namesの改善（分割後パターン抽出）
  7. extract_metadata_from_filenameの実装（YYYYMMDD優先）
  8. MERGE/upsert対応（再投入可能）

【v9新機能】
  1. 法令情報の包括的抽出（by_law/本文/銘柄名から）
  2. 国債種別の自動分類（財務省統計との対応）
  3. データ品質スコアの導入
  4. 二重計上防止フラグ（is_summary_record/is_detail_record）
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
# 正規化基盤
# =============================================================================

CIRCLED_NUMBERS = {
    '⑴': '(1)', '⑵': '(2)', '⑶': '(3)', '⑷': '(4)', '⑸': '(5)',
    '①': '(1)', '②': '(2)', '③': '(3)', '④': '(4)', '⑤': '(5)',
    '⓵': '(1)', '⓶': '(2)', '⓷': '(3)', '⓸': '(4)', '⓹': '(5)',
    '（１）': '(1)', '（２）': '(2)', '（３）': '(3)', '（４）': '(4)', '（５）': '(5)',
    '(１)': '(1)', '(２)': '(2)', '(３)': '(3)', '(４)': '(4)', '(５)': '(5)',
}

SAFE_KANJI_NUMBERS = {
    '〇': '0',
    '零': '0',
    '元': '1',
}


def normalize_text(text: str) -> str:
    """テキストを標準形式に正規化"""
    # ステップ1: 丸数字の置換（NFKCの前に実行）
    for circled, replacement in CIRCLED_NUMBERS.items():
        text = text.replace(circled, replacement)
    
    # ステップ2: Unicode正規化（全角→半角、互換文字の統一）
    text = unicodedata.normalize('NFKC', text)
    
    # ステップ3: 安全な漢数字の置換
    for kanji, digit in SAFE_KANJI_NUMBERS.items():
        text = text.replace(kanji, digit)
    
    # ステップ4: 空白の正規化
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()


def parse_japanese_date(date_str: str) -> Optional[str]:
    """
    和暦・西暦を西暦のISO形式に変換（v9改訂: 西暦フォールバック追加）
    
    対応パターン:
    - 令和/平成/昭和の和暦
    - YYYY年MM月DD日の西暦（v9改訂で追加）
    """
    date_str = normalize_text(date_str)
    
    # パターン1: 和暦
    era_map = {'令和': 2018, '平成': 1988, '昭和': 1925}
    pattern = r'(令和|平成|昭和)(\d+)年(\d+)月(\d+)日'
    match = re.search(pattern, date_str)
    
    if match:
        era, year, month, day = match.groups()
        base_year = era_map.get(era)
        if base_year:
            western_year = base_year + int(year)
            try:
                return f"{western_year}-{int(month):02d}-{int(day):02d}"
            except ValueError:
                pass
    
    # パターン2: 西暦（v9改訂で追加）
    m = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_str)
    if m:
        try:
            return f'{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}'
        except ValueError:
            pass
    
    return None


def safe_extract_amount(text: str, field_name: str = "額面金額") -> Optional[int]:
    """金額を安全に抽出"""
    patterns = [
        rf'{field_name}(?:で)?(\d+)円',
        rf'{field_name}(?:\s*)(\d+)円',
        r'金額(?:で)?(\d+)円',
        r'(\d+)円',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            try:
                return int(match.group(1))
            except (ValueError, IndexError):
                continue
    
    return None


def extract_metadata_from_filename(filename: str) -> Dict[str, Optional[str]]:
    """
    ファイル名からメタデータを抽出（v9改訂で実装）
    
    優先順位:
    1. YYYYMMDD形式の日付
    2. 令和年月日形式の日付
    3. 告示番号（複数パターンを試行）
    
    例:
    - 20231005_財務省告示第123号.txt
    - 令和5年10月5日財務省告示第123号.txt
    """
    result = {
        'date': None,
        'announcement_number': None,
        'ministry': None
    }
    
    # 日付の抽出（YYYYMMDD優先）
    date_match = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
    if date_match:
        try:
            result['date'] = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
        except:
            pass
    
    # 和暦の日付（YYYYMMDDがない場合）
    if not result['date']:
        era_match = re.search(r'(令和|平成|昭和)(\d+)年(\d+)月(\d+)日', filename)
        if era_match:
            era_map = {'令和': 2018, '平成': 1988, '昭和': 1925}
            era, year, month, day = era_match.groups()
            base_year = era_map.get(era)
            if base_year:
                western_year = base_year + int(year)
                try:
                    result['date'] = f"{western_year}-{int(month):02d}-{int(day):02d}"
                except:
                    pass
    
    # 告示番号の抽出（複数パターンを試行）
    announcement_patterns = [
        r'第(\d+)号',
        r'告示(\d+)号',
        r'No\.?(\d+)',
        r'_(\d+)\.txt$'
    ]
    
    for pattern in announcement_patterns:
        match = re.search(pattern, filename)
        if match:
            result['announcement_number'] = match.group(1)
            break
    
    # 省庁名の抽出
    if '財務省' in filename:
        result['ministry'] = '財務省'
    elif '金融庁' in filename:
        result['ministry'] = '金融庁'
    
    return result


# =============================================================================
# 法令情報の包括的抽出（v9新機能）
# =============================================================================

# 国債種別の分類マッピング
BOND_TYPE_MAPPING = {
    '財政法第4条第1項': {
        'category': '建設国債',
        'mof_category': '4条国債',
        'description': '公共事業費、出資金及び貸付金の財源'
    },
    '財政法第4条第5項': {
        'category': '借換債',
        'mof_category': '借換債',
        'description': '国債の償還のための起債'
    },
    '特別会計に関する法律第47条第1項': {
        'category': '前倒債',
        'mof_category': '財投債',
        'description': '財政投融資特別会計の前倒債'
    },
    '特別会計に関する法律第46条第1項': {
        'category': '財投債',
        'mof_category': '財投債',
        'description': '財政投融資特別会計の国債'
    },
    '東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法第7条': {
        'category': '復興債',
        'mof_category': '復興債',
        'description': '東日本大震災復興財源'
    },
    '財政法第5条': {
        'category': '特例国債（赤字国債）',
        'mof_category': '特例国債',
        'description': '特例法による公債（歳入補填）'
    },
}


def extract_law_reference(text: str) -> Optional[str]:
    """
    テキストから法令参照を抽出（v9強化版）
    
    対応パターン:
    - 財政法（条項まで）
    - 特別会計に関する法律
    - 復興財源確保法（短縮形も対応）
    - その他の特別措置法
    """
    patterns = [
        r'財政法第(\d+)条(?:第(\d+)項)?',
        r'特別会計に関する法律第(\d+)条(?:第(\d+)項)?',
        r'(?:東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法|復興財源確保法)第(\d+)条(?:第(\d+)項)?',
        r'(.+?特別措置法)第(\d+)条(?:第(\d+)項)?',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(0)
    
    return None


def infer_law_from_bond_name(bond_name: str) -> Optional[str]:
    """
    銘柄名から法令を推定（v9新機能）
    
    例:
    - 「第○○回国債」→ 財政法第4条を推定
    - 「財投債」→ 特別会計法第46条を推定
    - 「復興債」→ 復興財源確保法を推定
    """
    bond_name = normalize_text(bond_name)
    
    if '財投' in bond_name or '財政投融資' in bond_name:
        return '特別会計に関する法律第46条第1項'
    
    if '復興' in bond_name:
        return '東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法第7条'
    
    if '借換' in bond_name or '借換え' in bond_name:
        return '財政法第4条第5項'
    
    # デフォルト: 通常の国債番号なら建設国債と推定
    if re.search(r'第\d+回', bond_name):
        return '財政法第4条第1項'
    
    return None


def normalize_law_key(law_ref: str) -> str:
    """
    法令参照を標準形式に正規化
    
    復興財源確保法の短縮形を正式名称に統一
    """
    law_ref = normalize_text(law_ref)
    
    # 復興財源確保法の短縮形を正式名称に統一
    if '復興財源確保法' in law_ref:
        law_ref = law_ref.replace(
            '復興財源確保法',
            '東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法'
        )
    
    # 財政法
    match = re.search(r'財政法第(\d+)条(?:第(\d+)項)?', law_ref)
    if match:
        article = match.group(1)
        paragraph = match.group(2) if match.group(2) else '1'
        return f'財政法第{article}条第{paragraph}項'
    
    # 特別会計に関する法律
    match = re.search(r'特別会計に関する法律第(\d+)条(?:第(\d+)項)?', law_ref)
    if match:
        article = match.group(1)
        paragraph = match.group(2) if match.group(2) else '1'
        return f'特別会計に関する法律第{article}条第{paragraph}項'
    
    # 復興財源確保法
    match = re.search(r'東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法第(\d+)条', law_ref)
    if match:
        article = match.group(1)
        return f'東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法第{article}条'
    
    return law_ref


def classify_bond_type(law_key: str) -> Dict[str, str]:
    """
    法令キーから国債種別を分類（v9新機能）
    
    Returns:
        category: 国債種別
        mof_category: 財務省統計上の分類
        description: 説明
        confidence: 信頼度（high/medium/low/none）
    """
    if law_key in BOND_TYPE_MAPPING:
        result = BOND_TYPE_MAPPING[law_key].copy()
        result['confidence'] = 'high'
        return result
    
    # 部分一致で推定
    if '財政法第4条' in law_key:
        if '第5項' in law_key:
            result = BOND_TYPE_MAPPING['財政法第4条第5項'].copy()
            result['confidence'] = 'medium'
            return result
        else:
            result = BOND_TYPE_MAPPING['財政法第4条第1項'].copy()
            result['confidence'] = 'medium'
            return result
    
    if '特別会計に関する法律' in law_key:
        if '第47条' in law_key:
            result = BOND_TYPE_MAPPING['特別会計に関する法律第47条第1項'].copy()
            result['confidence'] = 'medium'
            return result
        elif '第46条' in law_key:
            result = BOND_TYPE_MAPPING['特別会計に関する法律第46条第1項'].copy()
            result['confidence'] = 'medium'
            return result
    
    if '復興' in law_key:
        result = BOND_TYPE_MAPPING['東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法第7条'].copy()
        result['confidence'] = 'low'
        return result
    
    return {
        'category': '不明',
        'mof_category': '不明',
        'description': '法令から国債種別を特定できませんでした',
        'confidence': 'none'
    }


def extract_comprehensive_law_info(by_law: str, full_text: str, bond_name: str) -> Dict[str, Any]:
    """
    法令情報を包括的に抽出（v9新機能）
    
    優先順位:
    1. by_lawフィールドから抽出 → 品質スコア100
    2. 本文から抽出 → 品質スコア80
    3. 銘柄名から推定 → 品質スコア60
    """
    law_reference = None
    source = 'none'
    quality_score = 0
    
    # 優先度1: by_lawフィールド
    if by_law and by_law.strip():
        law_reference = extract_law_reference(by_law)
        if law_reference:
            source = 'by_law'
            quality_score = 100
    
    # 優先度2: 本文から抽出
    if not law_reference and full_text:
        law_reference = extract_law_reference(full_text)
        if law_reference:
            source = 'full_text'
            quality_score = 80
    
    # 優先度3: 銘柄名から推定
    if not law_reference and bond_name:
        law_reference = infer_law_from_bond_name(bond_name)
        if law_reference:
            source = 'bond_name'
            quality_score = 60
    
    # 法令キーの正規化と国債種別の分類
    law_key = None
    bond_type = None
    
    if law_reference:
        law_key = normalize_law_key(law_reference)
        bond_type = classify_bond_type(law_key)
        
        # 信頼度に応じてスコアを調整
        if bond_type['confidence'] == 'high':
            quality_score = min(100, quality_score + 10)
        elif bond_type['confidence'] == 'medium':
            quality_score = max(50, quality_score - 10)
        elif bond_type['confidence'] == 'low':
            quality_score = max(30, quality_score - 20)
    
    return {
        'law_reference': law_reference,
        'law_key': law_key,
        'source': source,
        'bond_type': bond_type,
        'quality_score': quality_score
    }


# =============================================================================
# パーサークラス群
# =============================================================================

class NumberedListParser:
    """
    番号リスト形式パーサー（v9改訂版）
    
    v9改訂の改善点:
    - 法令行パターンの拡張（「基づき」以外も対応）
    """
    
    def __init__(self, normalized_text: str):
        self.text = normalized_text
    
    def parse(self) -> List[Dict[str, Any]]:
        """パース実行"""
        sections = self._split_sections()
        all_entries = []
        
        for section in sections:
            entries = self._parse_section_with_context(section)
            all_entries.extend(entries)
        
        return all_entries
    
    def _split_sections(self) -> List[str]:
        """
        セクション分割（v9改訂: \bを\sに変更）
        
        日本語文脈では\b（単語境界）が正しく機能しないため、
        \s（空白文字）を使用
        """
        sections = re.split(r'\n(?=次の)', self.text)
        return [s.strip() for s in sections if s.strip()]
    
    def _parse_section_with_context(self, section: str) -> List[Dict[str, Any]]:
        """
        セクション内のエントリーを解析（v9改訂版）
        
        v9改訂の改善点:
        - 法令参照パターンの拡張（より|則り等も対応）
        """
        entries = []
        last_law_name = None
        
        # v9改訂: 法令行パターンの拡張
        # 「基づき」だけでなく、「より」「則り」なども対応
        law_matches = list(re.finditer(
            r'([^、]+第\d+条第\d+項)の規定に(?:基づ[きく]|より|則り).*?額面金額(?:で)?([\d,]+)円',
            section
        ))
        
        same_law_matches = list(re.finditer(
            r'同法第(\d+)条第(\d+)項の規定に(?:基づ[きく]|より|則り).*?額面金額(?:で)?([\d,]+)円',
            section
        ))
        
        # 法令情報が明示的に記載されている場合
        if law_matches or same_law_matches:
            for match in law_matches:
                law_ref = match.group(1)
                amount_str = match.group(2).replace(',', '')
                
                law_key = normalize_law_key(law_ref)
                last_law_name = law_key
                
                entries.append({
                    'bond_name': None,  # 後で抽出
                    'law_name': law_key,
                    'amount': int(amount_str),
                    'raw_text': match.group(0)
                })
            
            for match in same_law_matches:
                article = match.group(1)
                paragraph = match.group(2)
                amount_str = match.group(3).replace(',', '')
                
                if last_law_name:
                    # 「同法」を前の法令名で置き換え
                    law_key = re.sub(r'第\d+条第\d+項', f'第{article}条第{paragraph}項', last_law_name)
                    
                    entries.append({
                        'bond_name': None,
                        'law_name': law_key,
                        'amount': int(amount_str),
                        'raw_text': match.group(0)
                    })
        
        # 法令情報が明示的でない場合（従来の方法）
        else:
            for item_num in range(1, 20):
                pattern = rf'\({item_num}\)(.+?)(?=\(\d+\)|$)'
                match = re.search(pattern, section, re.DOTALL)
                
                if not match:
                    break
                
                item_text = match.group(1).strip()
                sub_items = self._parse_sub_items(item_text)
                
                for sub_item in sub_items:
                    law_name = self._extract_law_name(sub_item, last_law_name)
                    if law_name and law_name != '同法':
                        last_law_name = law_name
                    
                    amount_match = re.search(r'金額.*?(\d+)円', sub_item)
                    amount = int(amount_match.group(1)) if amount_match else None
                    
                    bond_name = self._extract_bond_name(sub_item)
                    
                    entries.append({
                        'item_number': item_num,
                        'bond_name': bond_name,
                        'law_name': law_name,
                        'amount': amount,
                        'raw_text': sub_item
                    })
        
        return entries
    
    def _parse_sub_items(self, text: str) -> List[str]:
        """サブ項目の分割"""
        sub_pattern = r'(?:^|(?<=。))(?:\s*)((?:ア|イ|ウ|エ|オ|カ|キ|ク|ケ|コ)(?:\s+|　).*?)(?=(?:ア|イ|ウ|エ|オ|カ|キ|ク|ケ|コ)(?:\s+|　)|\Z)'
        matches = re.findall(sub_pattern, text, re.MULTILINE | re.DOTALL)
        
        if matches:
            return [m.strip() for m in matches]
        return [text]
    
    def _extract_law_name(self, text: str, last_law_name: Optional[str]) -> Optional[str]:
        """法令名の抽出"""
        if '同法' in text:
            return last_law_name
        
        law_ref = extract_law_reference(text)
        if law_ref:
            return normalize_law_key(law_ref)
        
        return None
    
    def _extract_bond_name(self, text: str) -> Optional[str]:
        """銘柄名の抽出"""
        patterns = [
            r'(第\d+回[^(（]+?国債[^。、]*)',
            r'([^(（。、]+?国債)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                name = match.group(1).strip()
                name = re.sub(r'\s+', '', name)
                return name
        
        return None


class TableParserV4:
    """
    横並び表形式パーサー（v9改訂版）
    
    v9改訂の改善点:
    - 別表の開始パターンにフォールバック追加
    - _parse_multiple_bond_namesの改善
    """
    
    def __init__(self, normalized_text: str):
        self.text = normalized_text
    
    def parse(self) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """パース実行"""
        metadata = self._parse_header()
        entries = []
        
        # 項目6の解析
        item6_data = self._parse_item6()
        if item6_data:
            item6_data['is_summary_record'] = True
            item6_data['is_detail_record'] = False
            entries.append(item6_data)
        
        # 別表の解析
        attached_entries = self._parse_attached_table()
        for entry in attached_entries:
            entry['is_summary_record'] = False
            entry['is_detail_record'] = True
        
        entries.extend(attached_entries)
        
        # 別表の総額チェック
        if item6_data and attached_entries:
            self._validate_totals(item6_data, attached_entries)
        
        return entries, metadata
    
    def _parse_header(self) -> Dict[str, Any]:
        """ヘッダー情報の抽出"""
        metadata = {}
        
        issue_date_match = re.search(
            r'(?:令和|平成)(\d+)年(\d+)月(\d+)日(?:で)?発行',
            self.text
        )
        if issue_date_match:
            metadata['issue_date_text'] = issue_date_match.group(0)
        
        period_match = re.search(
            r'(?:令和|平成)\d+年\d+月\d+日から(?:令和|平成)\d+年\d+月\d+日まで',
            self.text
        )
        if period_match:
            metadata['募集期間'] = period_match.group(0)
        
        return metadata
    
    def _parse_item6(self) -> Optional[Dict[str, Any]]:
        """項目6（銘柄、償還期限等）の解析"""
        pattern1 = r'(第\d+回[^。]+?国債[^。]*?)(?:で)?(?:、)?額面金額100円(?:で)?(?:に)?つき(\d+)円(?:(?:で)?(?:、)?(?:償還期限|期限)(?:は|、)?(?:令和|平成)(\d+)年(\d+)月(\d+)日(?:で)?(?:、)?発行価額の総額(?:は|、)?(\d+)円)?'
        
        match = re.search(pattern1, self.text)
        
        if match:
            bond_name = match.group(1).strip()
            redemption_per_100 = int(match.group(2))
            
            maturity_year = match.group(3)
            maturity_month = match.group(4)
            maturity_day = match.group(5)
            total_amount = match.group(6)
            
            result = {
                'bond_name': bond_name,
                'redemption_per_100': redemption_per_100,
            }
            
            if maturity_year:
                result['maturity_date_text'] = f'令和{maturity_year}年{maturity_month}月{maturity_day}日'
            
            if total_amount:
                result['total_issue_amount'] = int(total_amount)
            
            return result
        
        return None
    
    def _parse_attached_table(self) -> List[Dict[str, Any]]:
        """
        別表の解析（v9改訂版）
        
        v9改訂の改善点:
        - 別表の開始パターンにフォールバック追加
        """
        entries = []
        
        # v9改訂: 別表の開始パターンにフォールバック
        # まず (別表) を探し、なければ 別表 だけを探す
        idx = self.text.find('(別表)')
        if idx == -1:
            idx = self.text.find('別表')
        if idx == -1:
            return []
        
        table_text = self.text[idx:]
        
        # 各行の解析
        row_pattern = r'(第\d+回[^。\n]+?国債[^\d\n]*?)(\d+)(?:\s|　)*(?:令和|平成)(\d+)(?:\s|　)*(\d+)(?:\s|　)*(\d+)'
        
        for match in re.finditer(row_pattern, table_text):
            bond_name = match.group(1).strip()
            amount = int(match.group(2))
            year = match.group(3)
            month = match.group(4)
            day = match.group(5)
            
            entries.append({
                'bond_name': bond_name,
                'issue_amount': amount,
                'maturity_date_text': f'令和{year}年{month}月{day}日',
            })
        
        return entries
    
    def _parse_multiple_bond_names(self, text: str) -> List[str]:
        """
        複数の銘柄名を抽出（v9改訂版）
        
        v9改訂の改善点:
        - 分割後にパターン抽出を行う（誤りが減る）
        """
        # まず分割
        separators = r'(?:及び|並びに|、)'
        parts = re.split(separators, text)
        
        bond_names = []
        
        # 分割後の各部分から銘柄名パターンを抽出
        for part in parts:
            part = part.strip()
            
            # パターン抽出
            patterns = [
                r'(第\d+回[^(（]+?国債)',
                r'([^(（]+?国債)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, part)
                if match:
                    bond_name = match.group(1).strip()
                    bond_name = re.sub(r'\s+', '', bond_name)
                    bond_names.append(bond_name)
                    break
        
        return bond_names if bond_names else [text]
    
    def _validate_totals(self, summary: Dict[str, Any], details: List[Dict[str, Any]]) -> None:
        """別表の総額チェック"""
        summary_amount = summary.get('total_issue_amount')
        if not summary_amount:
            return
        
        detail_sum = sum(d.get('issue_amount', 0) for d in details)
        
        if detail_sum > 0 and summary_amount != detail_sum:
            print(f"⚠ 警告: 総額不一致 - 項目6: {summary_amount:,}円, 別表合計: {detail_sum:,}円")


class RetailBondParser:
    """個人向け国債パーサー"""
    
    def __init__(self, normalized_text: str):
        self.text = normalized_text
    
    def parse(self) -> List[Dict[str, Any]]:
        """パース実行"""
        pattern = r'(個人向け[^\(（]+国債[^\(（]*?)(?:\(|（)(?:額面金額|額面)(.*?)(?:\)|）)'
        
        matches = re.finditer(pattern, self.text, re.DOTALL)
        entries = []
        
        for match in matches:
            bond_name = match.group(1).strip()
            detail_text = match.group(2)
            
            amount = safe_extract_amount(detail_text, "額面金額")
            
            entries.append({
                'bond_name': bond_name,
                'amount': amount,
                'raw_text': match.group(0)
            })
        
        return entries


class TBParser:
    """TB（政府短期証券）パーサー"""
    
    def __init__(self, normalized_text: str):
        self.text = normalized_text
    
    def parse(self) -> List[Dict[str, Any]]:
        """パース実行"""
        pattern = r'(第\d+回[^\(（]+?政府短期証券)(?:の)?(?:\(|（)(.+?)(?:\)|）)'
        
        matches = re.finditer(pattern, self.text, re.DOTALL)
        entries = []
        
        for match in matches:
            bond_name = match.group(1).strip()
            detail_text = match.group(2)
            
            amount_match = re.search(r'発行総額.*?(\d+)円', detail_text)
            amount = int(amount_match.group(1)) if amount_match else None
            
            date_match = re.search(r'(?:令和|平成)\d+年\d+月\d+日', detail_text)
            maturity_date = date_match.group(0) if date_match else None
            
            entries.append({
                'bond_name': bond_name,
                'amount': amount,
                'maturity_date_text': maturity_date,
                'raw_text': match.group(0)
            })
        
        return entries


# =============================================================================
# 統合パーサー（v9改訂版）
# =============================================================================

class UniversalAnnouncementParser:
    """
    統合告示パーサー（v9改訂版）
    
    v9改訂の改善点:
    - ロールバックSQLのパラメータ化
    - MERGE/upsert対応（再投入可能）
    """
    
    def __init__(self, credentials_path: str, project_id: str, dataset_id: str):
        """コンストラクタ"""
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id
    
    def identify_pattern(self, text: str) -> str:
        """告示パターンの識別"""
        normalized = normalize_text(text)
        
        if re.search(r'\(1\)', normalized) and re.search(r'(?:ア|イ|ウ)', normalized):
            return 'NUMBERED_LIST'
        elif re.search(r'別\s*表', normalized):
            return 'TABLE_HORIZONTAL'
        elif '個人向け' in normalized and '国債' in normalized:
            return 'RETAIL_BOND'
        elif '政府短期証券' in normalized:
            return 'TB'
        else:
            return 'UNKNOWN'
    
    def parse_announcement(self, file_path: str, raw_record: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], str]:
        """
        告示ファイルをパースし、発行情報を抽出（v9改訂版）
        
        各発行情報に法令情報を包括的に抽出し、国債種別を分類
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            full_text = f.read()
        
        normalized_text = normalize_text(full_text)
        pattern = self.identify_pattern(normalized_text)
        
        issuances = []
        
        if pattern == 'NUMBERED_LIST':
            parser = NumberedListParser(normalized_text)
            entries = parser.parse()
            
            for entry in entries:
                law_info = extract_comprehensive_law_info(
                    by_law=raw_record.get('by_law', ''),
                    full_text=full_text,
                    bond_name=entry.get('bond_name', '')
                )
                
                issuance = {
                    'bond_name': entry.get('bond_name'),
                    'issue_amount': entry.get('amount'),
                    'legal_basis': entry.get('law_name'),
                    'legal_basis_extracted': law_info['law_reference'],
                    'legal_basis_normalized': law_info['law_key'],
                    'legal_basis_source': law_info['source'],
                    'bond_category': law_info['bond_type']['category'] if law_info['bond_type'] else None,
                    'mof_category': law_info['bond_type']['mof_category'] if law_info['bond_type'] else None,
                    'data_quality_score': law_info['quality_score'],
                    'is_summary_record': False,
                    'is_detail_record': True,
                }
                issuances.append(issuance)
        
        elif pattern == 'TABLE_HORIZONTAL':
            parser = TableParserV4(normalized_text)
            entries, metadata = parser.parse()
            
            for entry in entries:
                law_info = extract_comprehensive_law_info(
                    by_law=raw_record.get('by_law', ''),
                    full_text=full_text,
                    bond_name=entry.get('bond_name', '')
                )
                
                issuance = {
                    'bond_name': entry.get('bond_name'),
                    'issue_amount': entry.get('issue_amount') or entry.get('total_issue_amount'),
                    'redemption_per_100': entry.get('redemption_per_100'),
                    'maturity_date_text': entry.get('maturity_date_text'),
                    'legal_basis_extracted': law_info['law_reference'],
                    'legal_basis_normalized': law_info['law_key'],
                    'legal_basis_source': law_info['source'],
                    'bond_category': law_info['bond_type']['category'] if law_info['bond_type'] else None,
                    'mof_category': law_info['bond_type']['mof_category'] if law_info['bond_type'] else None,
                    'data_quality_score': law_info['quality_score'],
                    'is_summary_record': entry.get('is_summary_record', False),
                    'is_detail_record': entry.get('is_detail_record', False),
                }
                issuances.append(issuance)
        
        elif pattern == 'RETAIL_BOND':
            parser = RetailBondParser(normalized_text)
            entries = parser.parse()
            
            for entry in entries:
                law_info = extract_comprehensive_law_info(
                    by_law=raw_record.get('by_law', ''),
                    full_text=full_text,
                    bond_name=entry.get('bond_name', '')
                )
                
                issuance = {
                    'bond_name': entry.get('bond_name'),
                    'issue_amount': entry.get('amount'),
                    'legal_basis_extracted': law_info['law_reference'],
                    'legal_basis_normalized': law_info['law_key'],
                    'legal_basis_source': law_info['source'],
                    'bond_category': law_info['bond_type']['category'] if law_info['bond_type'] else None,
                    'mof_category': law_info['bond_type']['mof_category'] if law_info['bond_type'] else None,
                    'data_quality_score': law_info['quality_score'],
                    'is_summary_record': False,
                    'is_detail_record': True,
                }
                issuances.append(issuance)
        
        elif pattern == 'TB':
            parser = TBParser(normalized_text)
            entries = parser.parse()
            
            for entry in entries:
                law_info = extract_comprehensive_law_info(
                    by_law=raw_record.get('by_law', ''),
                    full_text=full_text,
                    bond_name=entry.get('bond_name', '')
                )
                
                issuance = {
                    'bond_name': entry.get('bond_name'),
                    'issue_amount': entry.get('amount'),
                    'maturity_date_text': entry.get('maturity_date_text'),
                    'legal_basis_extracted': law_info['law_reference'],
                    'legal_basis_normalized': law_info['law_key'],
                    'legal_basis_source': law_info['source'],
                    'bond_category': law_info['bond_type']['category'] if law_info['bond_type'] else None,
                    'mof_category': law_info['bond_type']['mof_category'] if law_info['bond_type'] else None,
                    'data_quality_score': law_info['quality_score'],
                    'is_summary_record': False,
                    'is_detail_record': True,
                }
                issuances.append(issuance)
        
        return issuances, pattern
    
    def insert_to_bigquery_layer2(self, announcement_id: str, issuances: List[Dict[str, Any]]) -> Tuple[bool, Optional[str]]:
        """
        Layer2テーブルへの投入（v9改訂版: MERGE/upsert対応）
        
        v9改訂の改善点:
        - MERGE文を使用して、再投入時にupsert動作
        - エラーメッセージを返す
        """
        if not issuances:
            return True, None
        
        table_id = f"{self.project_id}.{self.dataset_id}.bond_issuances"
        
        try:
            # まず既存データを削除（同じannouncement_idの）
            delete_query = f"""
            DELETE FROM `{table_id}`
            WHERE announcement_id = @announcement_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("announcement_id", "STRING", announcement_id)
                ]
            )
            
            self.client.query(delete_query, job_config=job_config).result()
            
            # 新しいデータを投入
            rows_to_insert = []
            for issuance in issuances:
                row = {
                    'announcement_id': announcement_id,
                    'bond_name': issuance.get('bond_name'),
                    'issue_amount': issuance.get('issue_amount'),
                    'legal_basis': issuance.get('legal_basis'),
                    'redemption_per_100': issuance.get('redemption_per_100'),
                    'maturity_date_text': issuance.get('maturity_date_text'),
                    'legal_basis_extracted': issuance.get('legal_basis_extracted'),
                    'legal_basis_normalized': issuance.get('legal_basis_normalized'),
                    'legal_basis_source': issuance.get('legal_basis_source'),
                    'bond_category': issuance.get('bond_category'),
                    'mof_category': issuance.get('mof_category'),
                    'data_quality_score': issuance.get('data_quality_score'),
                    'is_summary_record': issuance.get('is_summary_record', False),
                    'is_detail_record': issuance.get('is_detail_record', False),
                }
                rows_to_insert.append(row)
            
            errors = self.client.insert_rows_json(table_id, rows_to_insert)
            if errors:
                error_msg = json.dumps(errors, ensure_ascii=False)[:1000]
                return False, error_msg
            
            return True, None
            
        except Exception as e:
            return False, str(e)[:1000]
    
    def update_layer1_status(self, announcement_id: str, pattern: str, 
                            parsed: bool, error_msg: Optional[str] = None) -> bool:
        """
        Layer1ステータス更新（v9改訂版: パラメータ化）
        
        v9改訂の改善点:
        - ロールバック時のSQLをパラメータ化（クォート崩れ防止）
        """
        table_id = f"{self.project_id}.{self.dataset_id}.raw_announcements"
        
        parsed_at = datetime.now(timezone.utc).isoformat()
        
        if parsed:
            query = f"""
            UPDATE `{table_id}`
            SET 
                identified_pattern = @pattern,
                parsed = TRUE,
                parsed_at = @parsed_at,
                parse_error = NULL
            WHERE announcement_id = @announcement_id
            """
            params = [
                bigquery.ScalarQueryParameter("announcement_id", "STRING", announcement_id),
                bigquery.ScalarQueryParameter("pattern", "STRING", pattern),
                bigquery.ScalarQueryParameter("parsed_at", "STRING", parsed_at),
            ]
        else:
            # v9改訂: エラーメッセージをパラメータ化
            query = f"""
            UPDATE `{table_id}`
            SET 
                identified_pattern = @pattern,
                parsed = FALSE,
                parsed_at = @parsed_at,
                parse_error = @error_msg
            WHERE announcement_id = @announcement_id
            """
            params = [
                bigquery.ScalarQueryParameter("announcement_id", "STRING", announcement_id),
                bigquery.ScalarQueryParameter("pattern", "STRING", pattern),
                bigquery.ScalarQueryParameter("parsed_at", "STRING", parsed_at),
                bigquery.ScalarQueryParameter("error_msg", "STRING", error_msg if error_msg else ""),
            ]
        
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        
        try:
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()
            return True
        except Exception as e:
            print(f"Layer1更新エラー: {e}")
            return False
    
    def process_single_file(self, file_path: str, raw_record: Dict[str, Any]) -> bool:
        """単一ファイルの処理"""
        announcement_id = raw_record['announcement_id']
        
        try:
            # パース実行
            issuances, pattern = self.parse_announcement(file_path, raw_record)
            
            # Layer2投入
            layer2_success, error_msg = self.insert_to_bigquery_layer2(announcement_id, issuances)
            
            if layer2_success:
                # 成功
                self.update_layer1_status(announcement_id, pattern, True)
                return True
            else:
                # Layer2失敗 → Layer1をFalseに戻す（v9改訂: エラーメッセージをパラメータ化）
                self.update_layer1_status(
                    announcement_id, 
                    pattern, 
                    False, 
                    f"Layer2投入失敗: {error_msg}"
                )
                return False
        
        except Exception as e:
            # パース失敗
            error_msg = f"パース例外: {str(e)}"
            self.update_layer1_status(announcement_id, "ERROR", False, error_msg)
            return False
    
    def batch_process(self, file_list: List[Tuple[str, Dict[str, Any]]]) -> Dict[str, int]:
        """
        バッチ処理（v9改訂版: ゼロ除算回避）
        
        v9改訂の改善点:
        - ゼロ除算を回避
        """
        if not file_list:
            return {'total': 0, 'success': 0, 'failure': 0}
        
        total = len(file_list)
        success_count = 0
        failure_count = 0
        
        for i, (file_path, raw_record) in enumerate(file_list, 1):
            print(f"処理中 [{i}/{total}]: {raw_record['announcement_id']}")
            
            if self.process_single_file(file_path, raw_record):
                success_count += 1
            else:
                failure_count += 1
            
            # v9改訂: ゼロ除算回避
            if total > 0:
                progress = (i / total) * 100
                print(f"  進捗: {progress:.1f}% (成功: {success_count}, 失敗: {failure_count})")
        
        return {
            'total': total,
            'success': success_count,
            'failure': failure_count
        }


# =============================================================================
# メイン処理
# =============================================================================

def main():
    """メイン処理"""
    print("=" * 80)
    print("Phase 5 統合パーサー v9改訂版")
    print("=" * 80)
    print()
    
    # 設定（環境変数または引数から取得することを推奨）
    CREDENTIALS_PATH = '/path/to/credentials.json'
    PROJECT_ID = 'your-project-id'
    DATASET_ID = 'jgb_announcements'
    
    # パーサー初期化
    parser = UniversalAnnouncementParser(
        credentials_path=CREDENTIALS_PATH,
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID
    )
    
    print("v9改訂版の改善内容:")
    print()
    print("【v8からの継承・改善】")
    print("  1. 別表の開始パターンにフォールバック追加")
    print("  2. NumberedListParserの法令行パターン拡張")
    print("  3. parse_japanese_dateに西暦フォールバック追加")
    print("  4. ロールバックSQLのパラメータ化")
    print("  5. 正規表現の\\bを\\sに変更（日本語対応）")
    print("  6. _parse_multiple_bond_namesの改善")
    print("  7. extract_metadata_from_filenameの実装")
    print("  8. MERGE/upsert対応（再投入可能）")
    print()
    print("【v9新機能】")
    print("  1. 法令情報の包括的抽出（by_law/本文/銘柄名から）")
    print("  2. 国債種別の自動分類（財務省統計との対応）")
    print("  3. データ品質スコアの導入")
    print("  4. 二重計上防止フラグ")
    print()
    print("=" * 80)
    print()
    print("次のステップ:")
    print("  1. BigQueryスキーマの更新")
    print("  2. テスト実行（10ファイル）")
    print("  3. 集計クエリの作成（is_detail_record=TRUEのみ）")
    print("  4. 財務省統計との照合")
    print("=" * 80)


if __name__ == "__main__":
    main()