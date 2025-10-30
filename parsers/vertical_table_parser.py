"""
縦並び別表形式のパーサー
Day 4で追加
"""

import re
from typing import List, Dict, Optional
from datetime import datetime


class VerticalTableParser:
    """縦並び別表形式のパーサー
    
    2つのパターンに対応：
    - 4列形式：名称、利率、償還期限、発行額
    - 5列形式：名称、利率、償還期限、法令根拠、発行額
    """
    
    def __init__(self, notice_text: str):
        """
        Args:
            notice_text: 告示全文
        """
        self.notice_text = notice_text
        self.column_count = 0
        self.headers = []
        self.common_legal_basis = None
    
    def parse(self) -> List[Dict]:
        """縦並び別表から銘柄情報を抽出
        
        Returns:
            銘柄情報のリスト
        """
        # 1. 別表セクションを抽出
        table_section = self._extract_table_section()
        if not table_section:
            return []
        
        # 2. 共通法令根拠を抽出（4列形式の場合）
        self.common_legal_basis = self._extract_common_legal_basis()
        
        # 3. ページマーカーを除去
        cleaned_text = self._remove_page_markers(table_section)
        
        # 4. 行に分割
        lines = [line.strip() for line in cleaned_text.split('\n') if line.strip()]
        
        # 5. 列ヘッダーを検出
        header_end_idx = self._detect_headers(lines)
        if header_end_idx == -1:
            return []
        
        # 6. データ行を抽出
        data_lines = lines[header_end_idx + 1:]
        
        # 7. グループ化して銘柄を抽出
        issues = self._parse_data_lines(data_lines)
        
        return issues
    
    def _extract_table_section(self) -> Optional[str]:
        """別表セクションを抽出"""
        match = re.search(r'（別表）(.+?)(?:©2010|$)', self.notice_text, re.DOTALL)
        if match:
            return match.group(1)
        return None
    
    def _extract_common_legal_basis(self) -> Optional[str]:
        """本文から共通の法令根拠を抽出（4列形式用）"""
        # 「２　発行の根拠法律及びその条項」から抽出
        pattern = r'２\s+発行の根拠法律及びその条項\s+(.+?)(?:\n|３)'
        match = re.search(pattern, self.notice_text, re.DOTALL)
        if match:
            legal_text = match.group(1).strip()
            # 複数条項が含まれる場合は共通法令根拠としない
            if '及び' in legal_text and '条' in legal_text:
                return None
            return legal_text
        return None
    
    def _remove_page_markers(self, text: str) -> str:
        """ページマーカーを除去"""
        return re.sub(r'page="[0-9]+"', '', text)
    
    def _detect_headers(self, lines: List[str]) -> int:
        """列ヘッダーを検出
        
        Returns:
            ヘッダー終了行のインデックス（-1の場合は検出失敗）
        """
        # 期待される列ヘッダー
        expected_headers_4col = [
            '名称及び記号',
            '利率（年）',
            '償還期限',
            '発行額'
        ]
        
        expected_headers_5col = [
            '名称及び記号',
            '利率（年）',
            '償還期限',
            '発行の根拠法律及びその条項',
            '発行額'
        ]
        
        # ヘッダーを検索
        for i, line in enumerate(lines[:10]):  # 最初の10行以内で検索
            if '名称及び記号' in line:
                # この行から連続する行がヘッダー
                headers = []
                j = i
                while j < len(lines) and j < i + 10:
                    # データ行（利付国庫債券で始まる）が出現したら終了
                    if '利付国庫債券' in lines[j]:
                        break
                    if lines[j] and not lines[j].startswith('（'):
                        headers.append(lines[j])
                    j += 1
                
                # 列数を判定
                if len(headers) == 4:
                    self.column_count = 4
                    self.headers = headers
                    return j - 1
                elif len(headers) == 5:
                    self.column_count = 5
                    self.headers = headers
                    return j - 1
        
        return -1
    
    def _parse_data_lines(self, data_lines: List[str]) -> List[Dict]:
        """データ行から銘柄情報を抽出"""
        issues = []
        i = 0
        
        while i < len(data_lines):
            # 名称が2行に分かれているケースを検出
            # 例: 「利付国庫債券（20年）」と「（第167回）」が別行
            if i < len(data_lines) - 1:
                current_line = data_lines[i]
                next_line = data_lines[i + 1] if i + 1 < len(data_lines) else ""
                
                # パターン：1行目に「利付国庫債券（XX年）」、2行目に「（第YY回）」
                if ('利付国庫債券' in current_line and 
                    '年）' in current_line and 
                    '第' in next_line and 
                    '回）' in next_line and
                    current_line.count('（') == 1):  # 回号がない
                    
                    # 2行を結合
                    combined_name = current_line + next_line
                    data_lines[i] = combined_name
                    data_lines.pop(i + 1)  # 2行目を削除
            
            # 列数に応じてグループ化
            if self.column_count == 4:
                if i + 3 < len(data_lines):
                    issue = self._parse_4col_group(data_lines[i:i+4])
                    if issue:
                        issues.append(issue)
                    i += 4
                else:
                    break
            elif self.column_count == 5:
                if i + 4 < len(data_lines):
                    issue = self._parse_5col_group(data_lines[i:i+5])
                    if issue:
                        issues.append(issue)
                    i += 5
                else:
                    break
            else:
                break
        
        return issues
    
    def _parse_4col_group(self, lines: List[str]) -> Optional[Dict]:
        """4列グループから銘柄情報を抽出"""
        if len(lines) < 4:
            return None
        
        name_line = lines[0]
        rate_line = lines[1]
        maturity_line = lines[2]
        amount_line = lines[3]
        
        # 名称から情報を抽出
        name_info = self._parse_name(name_line)
        if not name_info:
            return None
        
        # 利率を抽出
        rate = self._parse_rate(rate_line)
        
        # 償還期限を抽出
        maturity_date = self._parse_maturity(maturity_line)
        
        # 発行額を抽出
        amount = self._parse_amount(amount_line)
        
        # 法令根拠は共通のものを使用
        legal_basis = self.common_legal_basis
        
        return {
            'name': name_line,
            'bond_type': name_info['bond_type'],
            'series_number': name_info['series_number'],
            'rate': rate,
            'maturity_date': maturity_date,
            'amount': amount,
            'legal_basis': legal_basis
        }
    
    def _parse_5col_group(self, lines: List[str]) -> Optional[Dict]:
        """5列グループから銘柄情報を抽出"""
        if len(lines) < 5:
            return None
        
        name_line = lines[0]
        rate_line = lines[1]
        maturity_line = lines[2]
        legal_basis_line = lines[3]
        amount_line = lines[4]
        
        # 名称から情報を抽出
        name_info = self._parse_name(name_line)
        if not name_info:
            return None
        
        # 利率を抽出
        rate = self._parse_rate(rate_line)
        
        # 償還期限を抽出
        maturity_date = self._parse_maturity(maturity_line)
        
        # 発行額を抽出
        amount = self._parse_amount(amount_line)
        
        # 法令根拠を抽出
        legal_basis = legal_basis_line
        
        return {
            'name': name_line,
            'bond_type': name_info['bond_type'],
            'series_number': name_info['series_number'],
            'rate': rate,
            'maturity_date': maturity_date,
            'amount': amount,
            'legal_basis': legal_basis
        }
    
    def _parse_name(self, name_line: str) -> Optional[Dict]:
        """銘柄名から種類と回号を抽出"""
        # 利付国庫債券（XX年）（第YY回）
        pattern = r'利付国庫債券（(\d+)年）（第(\d+)回）'
        match = re.search(pattern, name_line)
        if match:
            years = match.group(1)
            series = match.group(2)
            bond_type = f'{years}年'
            return {
                'bond_type': bond_type,
                'series_number': int(series)
            }
        return None
    
    def _parse_rate(self, rate_line: str) -> Optional[float]:
        """利率を抽出"""
        # 0.5％ → 0.5
        match = re.search(r'([\d.]+)％', rate_line)
        if match:
            return float(match.group(1))
        return None
    
    def _parse_maturity(self, maturity_line: str) -> Optional[datetime]:
        """償還期限を抽出"""
        # 令和20年12月20日 → datetime
        pattern = r'令和(\d+)年(\d+)月(\d+)日'
        match = re.search(pattern, maturity_line)
        if match:
            reiwa_year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            
            # 令和→西暦変換（令和元年=2019年）
            gregorian_year = 2018 + reiwa_year
            
            try:
                return datetime(gregorian_year, month, day)
            except ValueError:
                return None
        return None
    
    def _parse_amount(self, amount_line: str) -> Optional[int]:
        """発行額を抽出"""
        # 42,000,000,000円 → 42000000000
        amount_line = amount_line.replace('円', '').replace(',', '')
        match = re.search(r'(\d+)', amount_line)
        if match:
            return int(match.group(1))
        return None


def main():
    """メイン実行関数（テスト用）"""
    # テスト用のサンプルテキスト
    sample_text_5col = """
２　発行の根拠法律及びその条項
特別会計に関する法律第46条第１項及び第62条第１項
（別表）
名称及び記号
利率（年）
償還期限
発行の根拠法律及びその条項
発行額（額面金額）
page="0006"
利付国庫債券（20年）（第167回）
0.5％
令和20年12月20日
特別会計に関する法律第46条第１項分
42,000,000,000円
利付国庫債券（20年）（第171回）
0.3％
令和21年12月20日
特別会計に関する法律第46条第１項分
17,300,000,000円
page="0007"
利付国庫債券（30年）（第52回）
0.5％
令和28年９月20日
特別会計に関する法律第46条第１項分
1,400,000,000円
©2010
"""
    
    sample_text_4col = """
２　発行の根拠法律及びその条項
特別会計に関する法律第46条第１項
（別表）
名称及び記号
利率（年）
償還期限
発行額（額面金額）
利付国庫債券（10年）（第352回）
0.1％
令和10年９月20日
1,200,000,000円
利付国庫債券（10年）（第365回）
0.1％
令和13年12月20日
27,500,000,000円
©2010
"""
    
    print("=" * 50)
    print("5列形式のテスト")
    print("=" * 50)
    parser_5col = VerticalTableParser(sample_text_5col)
    issues_5col = parser_5col.parse()
    print(f"抽出された銘柄数: {len(issues_5col)}")
    for issue in issues_5col:
        print(f"\n{issue['name']}")
        print(f"  種類: {issue['bond_type']}")
        print(f"  回号: 第{issue['series_number']}回")
        print(f"  利率: {issue['rate']}%")
        print(f"  償還期限: {issue['maturity_date']}")
        print(f"  発行額: {issue['amount']:,}円")
        print(f"  法令根拠: {issue['legal_basis']}")
    
    print("\n" + "=" * 50)
    print("4列形式のテスト")
    print("=" * 50)
    parser_4col = VerticalTableParser(sample_text_4col)
    issues_4col = parser_4col.parse()
    print(f"抽出された銘柄数: {len(issues_4col)}")
    for issue in issues_4col:
        print(f"\n{issue['name']}")
        print(f"  種類: {issue['bond_type']}")
        print(f"  回号: 第{issue['series_number']}回")
        print(f"  利率: {issue['rate']}%")
        print(f"  償還期限: {issue['maturity_date']}")
        print(f"  発行額: {issue['amount']:,}円")
        print(f"  法令根拠: {issue['legal_basis']}")


if __name__ == '__main__':
    main()