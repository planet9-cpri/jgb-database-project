"""
NumberedListParser - 番号付きリスト形式の告示パーサー

対象形式:
１　名称及び記号　利付国庫債券（２年）（第447回）
２　発行の根拠法律及びその条項　特別会計に関する法律...
６　発行額
  ⑴　価格競争入札発行　額面金額で2,377,200,000,000円
  ⑵　国債市場特別参加者...　額面金額で522,100,000,000円
12　利率　年0.005％
15　償還期限　令和７年４月１日
"""

import re
from datetime import datetime
from typing import Optional, Dict, List


class NumberedListParser:
    """番号付きリスト形式の告示パーサー"""
    
    def __init__(self, notice_text: str):
        """
        Args:
            notice_text: 告示全文のテキスト
        """
        self.notice_text = notice_text
    
    def can_parse(self) -> bool:
        """
        このパーサーで処理できる形式かチェック
        
        Returns:
            True: 番号付きリスト形式
            False: 別の形式
        """
        # 「１　名称及び記号」で始まるかチェック
        pattern = r'^１\s+名称及び記号'
        return bool(re.search(pattern, self.notice_text, re.MULTILINE))
    
    def parse(self) -> Optional[Dict]:
        """
        番号付きリストから銘柄情報を抽出
        
        Returns:
            銘柄情報の辞書、または None
        """
        if not self.can_parse():
            return None
        
        # 各項目を抽出
        result = {
            'name': self._extract_name(),
            'bond_type': None,
            'series_number': None,
            'rate': self._extract_interest_rate(),
            'maturity_date': self._extract_maturity_date(),
            'amount': self._extract_total_amount(),
            'legal_basis': self._extract_legal_basis(),
        }
        
        # 名称から債券種類と回号を抽出
        if result['name']:
            bond_type_match = re.search(r'（(\d+)年）', result['name'])
            if bond_type_match:
                result['bond_type'] = f"{bond_type_match.group(1)}年"
            
            series_match = re.search(r'第(\d+)回', result['name'])
            if series_match:
                result['series_number'] = int(series_match.group(1))
        
        # 発行額が取得できなかった場合はNoneを返す
        if result['amount'] is None or result['amount'] == 0:
            return None
        
        return result
    
    def _extract_name(self) -> Optional[str]:
        """名称及び記号を抽出"""
        # パターン1: １　名称及び記号　利付国庫債券（２年）（第447回）
        pattern = r'１\s+名称及び記号\s+(.+?)(?:\n|$)'
        match = re.search(pattern, self.notice_text)
        
        if match:
            name = match.group(1).strip()
            # 余分な空白を削除
            name = re.sub(r'\s+', '', name)
            return name
        
        return None
    
    def _extract_interest_rate(self) -> Optional[float]:
        """利率を抽出"""
        # パターン: 12　利率　年0.005％
        # または: 11　利率　年0.2％
        pattern = r'\d+\s+利\s*率\s+年([\d.]+)％'
        match = re.search(pattern, self.notice_text)
        
        if match:
            return float(match.group(1))
        
        return None
    
    def _extract_maturity_date(self) -> Optional[datetime]:
        """償還期限を抽出"""
        # パターン: 15　償還期限　令和７年４月１日
        pattern = r'\d+\s+償還期限\s+令和(\d+)年(\d+)月(\d+)日'
        match = re.search(pattern, self.notice_text)
        
        if match:
            reiwa_year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            
            # 令和 → 西暦変換（令和元年 = 2019年）
            gregorian_year = 2018 + reiwa_year
            
            try:
                return datetime(gregorian_year, month, day)
            except ValueError:
                return None
        
        return None
    
    def _extract_total_amount(self) -> Optional[int]:
        """
        発行額を抽出（複数の入札方式がある場合は合計）
        
        例:
        ６　発行額
          ⑴　価格競争入札発行　額面金額で2,377,200,000,000円
          ⑵　国債市場特別参加者...　額面金額で522,100,000,000円
        """
        # 「６　発行額」セクションを見つける
        amount_section_pattern = r'６\s+発\s*行\s*額(.+?)(?=\d+\s+[^\s⑴⑵⑶]|$)'
        amount_section = re.search(amount_section_pattern, self.notice_text, re.DOTALL)
        
        if not amount_section:
            return None
        
        section_text = amount_section.group(1)
        
        # 「額面金額で」に続く数字を抽出（カンマ区切りあり）
        amount_pattern = r'額面金額で([\d,]+)円'
        amounts = re.findall(amount_pattern, section_text)
        
        if not amounts:
            return None
        
        # すべての発行額を合計
        total = 0
        for amount_str in amounts:
            # カンマを削除して整数に変換
            amount_str = amount_str.replace(',', '')
            total += int(amount_str)
        
        return total
    
    def _extract_legal_basis(self) -> Optional[str]:
        """法的根拠を抽出"""
        # パターン: ２　発行の根拠法律及びその条項　特別会計に関する法律（平成19年法律第23号）第46条第１項
        pattern = r'２\s+発行の根拠法律及びその条項\s+(.+?)(?:\n|$)'
        match = re.search(pattern, self.notice_text, re.DOTALL)
        
        if match:
            legal_basis = match.group(1).strip()
            # 複数行にまたがる場合があるので、改行を削除
            legal_basis = re.sub(r'\s+', '', legal_basis)
            return legal_basis
        
        return None
    
    @staticmethod
    def extract_from_file(filepath: str) -> Optional[Dict]:
        """
        ファイルから直接銘柄を抽出
        
        Args:
            filepath: 告示ファイルのパス
            
        Returns:
            銘柄情報の辞書、または None
        """
        from pathlib import Path
        
        filepath = Path(filepath)
        if not filepath.exists():
            return None
        
        with open(filepath, 'r', encoding='utf-8') as f:
            notice_text = f.read()
        
        parser = NumberedListParser(notice_text)
        return parser.parse()


# ========================================
# テストコード
# ========================================

if __name__ == '__main__':
    import sys
    from pathlib import Path
    
    # テストファイル
    test_files = [
        r"G:\マイドライブ\JGBデータ\2023\20230403_令和5年5月9日付（財務省第百二十一号）.txt",
        r"G:\マイドライブ\JGBデータ\2023\20230405_令和5年5月9日付（財務省第百二十三号）.txt",
    ]
    
    print("=" * 80)
    print("NumberedListParser テスト")
    print("=" * 80)
    
    for test_file in test_files:
        filepath = Path(test_file)
        
        if not filepath.exists():
            print(f"\n❌ ファイルが見つかりません: {filepath.name}")
            continue
        
        print(f"\n{'='*80}")
        print(f"📄 ファイル: {filepath.name}")
        print(f"{'='*80}")
        
        # パース実行
        result = NumberedListParser.extract_from_file(filepath)
        
        if result:
            print(f"✅ 抽出成功")
            print(f"\n【抽出結果】")
            print(f"  名称: {result['name']}")
            print(f"  種類: {result['bond_type']}")
            print(f"  回号: 第{result['series_number']}回")
            print(f"  利率: {result['rate']}%")
            print(f"  償還期限: {result['maturity_date'].strftime('%Y年%m月%d日') if result['maturity_date'] else 'なし'}")
            print(f"  発行額: {result['amount']:,}円 ({result['amount']/1000000000000:.2f}兆円)")
            print(f"  法的根拠: {result['legal_basis'][:50]}..." if result['legal_basis'] and len(result['legal_basis']) > 50 else f"  法的根拠: {result['legal_basis']}")
        else:
            print(f"❌ 抽出失敗")
    
    print(f"\n{'='*80}")
    print("テスト完了")
    print(f"{'='*80}")