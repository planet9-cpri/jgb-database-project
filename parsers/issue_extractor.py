"""
IssueExtractor v2 - 番号付きリスト形式対応版

処理フロー:
1. 番号付きリスト形式（NumberedListParser）で試行
2. 失敗した場合、横並び形式（TableParser）で試行
3. 失敗した場合、縦並び形式（VerticalTableParser）で試行
"""

import re
import logging
from typing import List, Dict, Optional
from pathlib import Path

# 相対インポートと絶対インポートの切り替え
try:
    from .table_parser import TableParser
    from .vertical_table_parser import VerticalTableParser
    from .numbered_list_parser import NumberedListParser
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from parsers.table_parser import TableParser
    from parsers.vertical_table_parser import VerticalTableParser
    from parsers.numbered_list_parser import NumberedListParser

logger = logging.getLogger(__name__)


class IssueExtractor:
    """
    別表テキストから銘柄情報を抽出する統合クラス（v2）
    
    処理フロー:
    1. 番号付きリスト形式で試行（NEW）
    2. 横並び形式で試行
    3. 縦並び形式で試行
    """
    
    def __init__(self, notice_text: str):
        """
        Args:
            notice_text: 告示全文のテキスト
        """
        self.notice_text = notice_text
    
    def extract_issues(self) -> List[Dict]:
        """
        銘柄情報を抽出（全形式対応）
        
        Returns:
            銘柄情報のリスト（統一された辞書形式）
        """
        # 1. 番号付きリスト形式（NumberedListParser）で試行
        logger.info("番号付きリスト形式で銘柄抽出を試行...")
        numbered_parser = NumberedListParser(self.notice_text)
        
        if numbered_parser.can_parse():
            result = numbered_parser.parse()
            if result:
                logger.info(f"✅ 番号付きリスト形式で抽出成功：1銘柄")
                return [result]
        
        # 2. 横並び形式（TableParser）で試行
        logger.info("横並び形式で銘柄抽出を試行...")
        table_parser = TableParser()
        bond_issuances = table_parser.parse_table(self.notice_text)
        
        if bond_issuances:
            logger.info(f"✅ 横並び形式で抽出成功：{len(bond_issuances)}銘柄")
            return self._convert_bond_issuances_to_dicts(bond_issuances)
        
        # 3. 縦並び形式（VerticalTableParser）で試行
        logger.info("⚠️ 横並び形式では抽出できませんでした。縦並び形式を試行...")
        vertical_parser = VerticalTableParser(self.notice_text)
        issues = vertical_parser.parse()
        
        if issues:
            logger.info(f"✅ 縦並び形式で抽出成功：{len(issues)}銘柄")
            return issues
        
        # 4. すべて失敗
        logger.warning("❌ 銘柄を抽出できませんでした")
        return []
    
    def _convert_bond_issuances_to_dicts(self, bond_issuances: List) -> List[Dict]:
        """
        BondIssuance（dataclass）を辞書形式に変換
        """
        results = []
        for bond in bond_issuances:
            # series_numberから数値を抽出（"第167回" → 167）
            series_num_match = re.search(r'第(\d+)回', bond.series_number)
            series_num = int(series_num_match.group(1)) if series_num_match else 0
            
            # maturity_dateをdatetimeに変換
            maturity_datetime = self._parse_wareki_date(bond.maturity_date)
            
            results.append({
                'name': bond.bond_name,
                'bond_type': f'{bond.bond_type}年',
                'series_number': series_num,
                'rate': bond.interest_rate,
                'maturity_date': maturity_datetime,
                'amount': int(bond.face_value_individual),
                'legal_basis': bond.legal_basis
            })
        
        return results
    
    def _parse_wareki_date(self, date_str: str):
        """和暦日付をdatetimeに変換"""
        from datetime import datetime
        
        if not date_str or date_str == "不明":
            return None
        
        match = re.search(r'令和(\d+)年(\d+)月(\d+)日', date_str)
        if match:
            reiwa_year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            gregorian_year = 2018 + reiwa_year
            
            try:
                return datetime(gregorian_year, month, day)
            except ValueError:
                return None
        
        return None
    
    @staticmethod
    def extract_from_file(filepath: str) -> List[Dict]:
        """
        ファイルから直接銘柄を抽出
        """
        filepath = Path(filepath)
        
        if not filepath.exists():
            logger.error(f"ファイルが見つかりません: {filepath}")
            return []
        
        with open(filepath, 'r', encoding='utf-8') as f:
            notice_text = f.read()
        
        extractor = IssueExtractor(notice_text)
        return extractor.extract_issues()


# テスト用コード
if __name__ == '__main__':
    import sys
    
    # ロギング設定
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # テストファイル
    test_cases = [
        {
            'file': r"G:\マイドライブ\JGBデータ\2023\20230403_令和5年5月9日付（財務省第百二十一号）.txt",
            'description': "番号付きリスト（2年債）"
        },
        {
            'file': r"G:\マイドライブ\JGBデータ\2023\20230405_令和5年5月9日付（財務省第百二十三号）.txt",
            'description': "番号付きリスト（10年債）"
        },
        {
            'file': r"G:\マイドライブ\JGBデータ\2023\20230414_令和5年5月9日付（財務省第百二十七号）.txt",
            'description': "縦並び5列形式"
        },
    ]
    
    print("=" * 70)
    print("🚀 IssueExtractor v2 テスト")
    print("=" * 70)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{'='*70}")
        print(f"📄 テストケース {i}: {test_case['description']}")
        print(f"{'='*70}")
        print(f"ファイル: {Path(test_case['file']).name}")
        
        # ファイル読み込み
        filepath = Path(test_case['file'])
        if not filepath.exists():
            print(f"❌ ファイルが見つかりません: {filepath}")
            continue
        
        with open(filepath, 'r', encoding='utf-8') as f:
            notice_text = f.read()
        
        # 抽出実行
        extractor = IssueExtractor(notice_text)
        issues = extractor.extract_issues()
        
        # 結果表示
        print(f"\n【抽出結果】")
        print(f"  抽出された銘柄数: {len(issues)}")
        
        if issues:
            print(f"\n  最初の銘柄:")
            issue = issues[0]
            print(f"    名称: {issue.get('name', 'N/A')}")
            print(f"    種類: {issue.get('bond_type', 'N/A')}")
            print(f"    回号: 第{issue.get('series_number', 'N/A')}回")
            print(f"    利率: {issue.get('rate', 'N/A')}%")
            if issue.get('amount'):
                print(f"    発行額: {issue['amount']:,}円 ({issue['amount']/1000000000000:.2f}兆円)")
            
            if len(issues) > 1:
                print(f"\n    ... 他{len(issues) - 1}銘柄")
    
    print(f"\n{'='*70}")
    print("テスト完了")
    print(f"{'='*70}")