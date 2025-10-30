"""
横並び別表形式のパーサー
Phase 4 用に 3層アーキテクチャに対応
"""

import re
from typing import List, Dict, Any


class TableParserV4:
    """横並び別表形式の告示を解析"""
    
    def can_parse(self, text: str) -> bool:
        """このパーサーで処理可能か判定"""
        # 別表の存在確認
        if '（別表のとおり）' in text or '内訳（別表のとおり）' in text:
            # 別表の実際のデータがあるか確認
            if '名称及び記号' in text and '利率' in text and '償還期限' in text:
                return True
        return False
    
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
        match = re.search(r'６\s+発行額\s+額面金額で([\d,]+)円\s*内訳（別表のとおり）', text, re.DOTALL)
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
        
        return items
    
    def _parse_multiple_bond_names(self, text: str) -> List[str]:
        """複数の銘柄名を抽出"""
        # 例: "利付国庫債券（20年）（第167回、第171回、第179回及び第181回）"
        bond_names = []
        
        # パターン1: 「及び」で区切られた複数の債券種類
        parts = re.split(r'及び', text)
        
        for part in parts:
            part = part.strip()
            if '利付国庫債券' in part:
                bond_names.append(part)
        
        return bond_names
    
    def _parse_laws(self, text: str) -> Dict[str, Any]:
        """法令を抽出"""
        laws = []
        
        # 全角→半角
        text_normalized = text.replace('４', '4').replace('６', '6').replace('２', '2').replace('１', '1')
        
        # 特別会計法第46条
        if re.search(r'特別会計.*?第46条第1項', text_normalized, re.DOTALL):
            laws.append({
                'name': '特別会計に関する法律',
                'full_name': '特別会計に関する法律（平成19年法律第23号）',
                'article': '第46条第1項',
                'key': '特別会計に関する法律第46条第1項',
                'amount': 0
            })
        
        # 特別会計法第62条
        if re.search(r'特別会計.*?第62条第1項', text_normalized, re.DOTALL):
            laws.append({
                'name': '特別会計に関する法律',
                'full_name': '特別会計に関する法律（平成19年法律第23号）',
                'article': '第62条第1項',
                'key': '特別会計に関する法律第62条第1項',
                'amount': 0
            })
        
        return {'laws': laws}
    
    def _parse_date(self, text: str) -> Dict[str, Any]:
        """発行日を解析"""
        match = re.search(r'令和(\d+)年(\d+)月(\d+)日', text)
        if match:
            year = int(match.group(1)) + 2018
            month = int(match.group(2))
            day = int(match.group(3))
            return {'issue_date': f'{year}-{month:02d}-{day:02d}'}
        return {'raw': text}
    
    def _parse_table(self, text: str) -> List[Dict[str, Any]]:
        """別表を解析"""
        
        # 別表の開始位置を探す
        table_start = text.find('（別表）')
        if table_start == -1:
            return []
        
        table_text = text[table_start:]
        
        # 表の各行を抽出
        rows = []
        
        # パターン: 利付国庫債券（XX年）（第XX回）\n利率\n償還期限\n法令\n発行額
        pattern = r'利付国庫債券（(.+?)）（第(\d+)回）\s+([\d.]+)％\s+令和(\d+)年(\d+)月(\d+)日\s+特別会計に関する法律第(\d+)条第１項分\s+([\d,]+)円'
        
        matches = re.finditer(pattern, table_text)
        
        for match in matches:
            bond_type = match.group(1)  # "20年", "30年", "40年"
            series = match.group(2)
            rate = float(match.group(3))
            maturity_year = int(match.group(4)) + 2018
            maturity_month = int(match.group(5))
            maturity_day = int(match.group(6))
            law_article = match.group(7)  # "46" or "62"
            amount = int(match.group(8).replace(',', ''))
            
            # 法令のキー
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
    
    def _format_table_row(self, row: Dict[str, Any]) -> str:
        """表の行を文字列にフォーマット"""
        return (f"{row['bond_name']}{row['bond_series']}: "
                f"利率{row['interest_rate']}%, "
                f"償還期限{row['maturity_date']}, "
                f"{row['law_key']}, "
                f"発行額{row['issue_amount']:,}円")


# テスト用
if __name__ == "__main__":
    # サンプルテキスト（実際のファイルから）
    sample_text = """
    ６　　　　発行額　　　　額面金額で499,500,000,000円
    内訳（別表のとおり）
    
    （別表）
    名称及び記号
    利率
    （年）
    償還期限
    発行の根拠法律及びその条項
    発行額
    （額面金額）
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
    """
    
    parser = TableParserV4()
    if parser.can_parse(sample_text):
        print("✅ このパーサーで処理可能")
        items = parser.extract(sample_text)
        print(f"抽出項目数: {len(items)}")
    else:
        print("❌ このパーサーでは処理不可")