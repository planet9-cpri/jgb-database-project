# parsers/table_parser.py
"""
TableParser - 別表から国債銘柄情報を抽出

複数銘柄の一括発行告示に含まれる別表をパースします。

対応フォーマット:
- 5列構成: 名称、利率、償還期限、発行根拠、発行額（縦に5行）
- 4列構成: 名称、利率、償還期限、発行額（縦に4行）
- 単一銘柄: 利付国債、物価連動国債、GX債券、国庫短期証券、個人向け国債
"""

import re
from typing import List, Optional, Dict
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class BondIssuance:
    """国債発行情報"""
    sub_index: int                          # 別表内の連番
    bond_name: str                          # 銘柄名
    bond_type: str                          # 債券種別（例: 20年）
    series_number: str                      # 回号（例: 第167回）
    interest_rate: float                    # 利率（例: 0.5）
    maturity_date: str                      # 償還期限（例: 令和20年12月20日）
    legal_basis: str                        # 発行根拠
    face_value_individual: float            # 発行額（円）


class TableParser:
    """別表解析パーサー"""
    
    # 正規表現パターン（最終版 - 8パターン対応）
    PATTERNS = {
        'table_start': r'（別\s*表）',
        # 利付国庫債券のパターン（空白対応）
        'bond_name_ritsuki': r'利\s*付国庫債券[（\(](\d+)年[）\)][（\(]第(\d+)回[）\)]',
        # 物価連動国債のパターン
        'bond_name_bukka': r'利付国庫債券[（\(]物価連動・(\d+)年[）\)][（\(]第(\d+)回[）\)]',
        # GX債券（クライメート・トランジション）のパターン
        'bond_name_gx': r'ク\s*ライメート・トランジション利付国庫債券[（\(](\d+)年[）\)][（\(]第(\d+)回[）\)]',
        # 国庫短期証券のパターン
        'bond_name_tanki': r'国庫短期証券[（\(]第(\d+)回[）\)]',
        # 個人向け利付国庫債券のパターン
        'bond_name_kojin': r'個\s*人向け利付国庫債券[（\(](.+?)[）\)][（\(]第(\d+)回[）\)]',
        'interest_rate': r'([\d.]+)％',
        'amount': r'([\d,]+)円',
        'wareki_date': r'令和(\d+)年(\d+)月(\d+)日',
        'legal_basis': r'特別会計に関する法律第(\d+)条第(\d+)項',
    }
    
    def __init__(self):
        """初期化"""
        pass
    
    def parse_table(self, content: str, common_legal_basis: Optional[str] = None) -> List[BondIssuance]:
        """
        本文から別表を抽出し、銘柄情報をパース
        
        Args:
            content: 官報本文
            common_legal_basis: 共通の発行根拠（4列フォーマット用）
            
        Returns:
            銘柄情報のリスト
        """
        bonds = []
        
        # 別表の開始位置を検索
        table_match = re.search(self.PATTERNS['table_start'], content)
        if not table_match:
            logger.info("別表が見つかりません")
            return bonds
        
        # 別表部分を抽出
        table_start = table_match.start()
        table_text = content[table_start:]
        
        # 行に分割
        lines = table_text.split('\n')
        
        # ヘッダー行を探す
        header_idx = None
        for i, line in enumerate(lines):
            if '名称及び記号' in line:
                header_idx = i
                break
        
        if header_idx is None:
            logger.warning("別表のヘッダーが見つかりません")
            return bonds
        
        # フォーマットを判定（5列 or 4列）
        format_type = self._detect_table_format(lines[header_idx:header_idx+10])
        logger.info(f"別表フォーマット: {format_type}列")
        
        # 4列の場合、本文から共通の発行根拠を抽出
        if format_type == 4 and not common_legal_basis:
            common_legal_basis = self._extract_common_legal_basis(content)
            logger.info(f"共通発行根拠: {common_legal_basis}")
        
        # データ行を解析
        sub_index = 0
        i = header_idx + 1
        
        while i < len(lines):
            line = lines[i].strip()
            
            # 空行やページ番号はスキップ
            if not line or 'page=' in line or '©' in line:
                i += 1
                continue
            
            # ヘッダーの各カラム名はスキップ
            if line in ['利率', '（年）', '償還期限', '発行額', '（額面金額）', 
                       '発行の根拠法律及びその条項', '利率（年）', '名称及び記号']:
                i += 1
                continue
            
            # 銘柄行かどうかチェック
            if '利付国庫債券' in line:
                sub_index += 1
                
                # 次の数行を取得
                rows_needed = format_type
                data_lines = []
                
                for j in range(rows_needed):
                    if i + j < len(lines):
                        data_lines.append(lines[i + j].strip())
                
                # データ行をパース
                bond = self._parse_multiline_bond(
                    data_lines,
                    sub_index,
                    format_type,
                    common_legal_basis
                )
                
                if bond:
                    bonds.append(bond)
                
                # 次の銘柄へ移動
                i += rows_needed
            else:
                i += 1
        
        logger.info(f"別表から{len(bonds)}銘柄を抽出しました")
        return bonds
    
    def _detect_table_format(self, header_lines: List[str]) -> int:
        """
        ヘッダー付近の行から別表のフォーマットを判定
        
        Returns:
            4 or 5（列数）
        """
        text = '\n'.join(header_lines)
        
        if '発行の根拠法律' in text:
            return 5
        else:
            return 4
    
    def _extract_common_legal_basis(self, content: str) -> Optional[str]:
        """
        本文から共通の発行根拠を抽出（4列フォーマット用）
        """
        pattern1 = r'[２2]\s*発行の根拠法律及びその条項\s+(.+?)(?:\n[３3]|\n\n)'
        match = re.search(pattern1, content, re.DOTALL)
        if match:
            legal = match.group(1).strip()
            # 改行を空白に置換
            return re.sub(r'\s+', '', legal)
        
        return None
    
    def _parse_multiline_bond(
        self,
        lines: List[str],
        sub_index: int,
        format_type: int,
        common_legal_basis: Optional[str]
    ) -> Optional[BondIssuance]:
        """
        複数行にまたがる銘柄情報をパース
        
        Args:
            lines: 銘柄の各行データ（4行 or 5行）
            sub_index: 連番
            format_type: 4 or 5
            common_legal_basis: 共通の発行根拠
        """
        try:
            if format_type == 5:
                if len(lines) < 5:
                    return None
                name_line = lines[0]
                rate_line = lines[1]
                date_line = lines[2]
                legal_line = lines[3]
                amount_line = lines[4]
            else:  # 4列
                if len(lines) < 4:
                    return None
                name_line = lines[0]
                rate_line = lines[1]
                date_line = lines[2]
                amount_line = lines[3]
                legal_line = None
            
            # 銘柄名の抽出
            bond_match = re.search(self.PATTERNS['bond_name_ritsuki'], name_line)
            if not bond_match:
                return None
            
            bond_type = bond_match.group(1)
            series_number = f"第{bond_match.group(2)}回"
            bond_name = bond_match.group(0)
            
            # 利率の抽出
            rate_match = re.search(self.PATTERNS['interest_rate'], rate_line)
            interest_rate = float(rate_match.group(1)) if rate_match else 0.0
            
            # 償還期限の抽出
            maturity_date = date_line if '令和' in date_line else "不明"
            
            # 発行額の抽出
            amount_match = re.search(self.PATTERNS['amount'], amount_line)
            if amount_match:
                amount_str = amount_match.group(1).replace(',', '')
                face_value = float(amount_str)
            else:
                face_value = 0.0
            
            # 発行根拠の抽出
            if format_type == 5 and legal_line:
                legal_basis = legal_line
            else:
                legal_basis = common_legal_basis or "不明"
            
            return BondIssuance(
                sub_index=sub_index,
                bond_name=bond_name,
                bond_type=bond_type,
                series_number=series_number,
                interest_rate=interest_rate,
                maturity_date=maturity_date,
                legal_basis=legal_basis,
                face_value_individual=face_value
            )
            
        except Exception as e:
            logger.error(f"銘柄パースエラー: {e}")
            return None
    
    def extract_bond_info_from_single(self, content: str) -> Optional[BondIssuance]:
        """
        単一銘柄の告示から銘柄情報を抽出

        対応銘柄:
        - 利付国庫債券
        - 物価連動国債
        - GX債券（クライメート・トランジション）
        - 国庫短期証券
        - 個人向け利付国庫債券
        """
        try:
            # 銘柄名の抽出（"１　名称及び記号　..."）
            # 全角・半角両対応、改行をまたぐ
            name_match = re.search(
                r'[１1]\s*名称及び記号\s*(.+?)(?:[２2]\s*発行の根拠|$)', 
                content, 
                re.DOTALL
            )
            if not name_match:
                logger.warning("銘柄名が見つかりません")
                return None
            
            bond_name_text = name_match.group(1).strip()
            # 複数の空白・改行を削除
            bond_name_text = re.sub(r'\s+', '', bond_name_text)
            
            # 複数パターンで銘柄名を解析
            bond_type = "不明"
            series_number = "不明"
            bond_name = bond_name_text
            
            # パターン1: GX債券（クライメート・トランジション）
            bond_match = re.search(self.PATTERNS['bond_name_gx'], bond_name_text)
            if bond_match:
                bond_type = bond_match.group(1)
                series_number = f"第{bond_match.group(2)}回"
                bond_name = bond_match.group(0)
            else:
                # パターン2: 物価連動国債
                bond_match = re.search(self.PATTERNS['bond_name_bukka'], bond_name_text)
                if bond_match:
                    bond_type = f"物価連動・{bond_match.group(1)}"
                    series_number = f"第{bond_match.group(2)}回"
                    bond_name = bond_match.group(0)
                else:
                    # パターン3: 通常の利付国庫債券
                    bond_match = re.search(self.PATTERNS['bond_name_ritsuki'], bond_name_text)
                    if bond_match:
                        bond_type = bond_match.group(1)
                        series_number = f"第{bond_match.group(2)}回"
                        bond_name = bond_match.group(0)
                    else:
                        # パターン4: 国庫短期証券
                        bond_match = re.search(self.PATTERNS['bond_name_tanki'], bond_name_text)
                        if bond_match:
                            bond_type = "短期"
                            series_number = f"第{bond_match.group(1)}回"
                            bond_name = bond_match.group(0)
                        else:
                            # パターン5: 個人向け利付国庫債券
                            bond_match = re.search(self.PATTERNS['bond_name_kojin'], bond_name_text)
                            if bond_match:
                                bond_type = bond_match.group(1)  # 例: 変動・10年
                                series_number = f"第{bond_match.group(2)}回"
                                bond_name = bond_match.group(0)
            
            # 利率の抽出（複数パターン対応）
            interest_rate = 0.0
            
            # パターン1: "12　利率　年0.7％"
            rate_match = re.search(r'1[012]\s+(?:利率|　利率)\s+年([\d.]+)％', content)
            if rate_match:
                interest_rate = float(rate_match.group(1))
            else:
                # パターン2: "９　初期利子の適用利率　年0.28％"（個人向け）
                rate_match = re.search(r'初期利子の適用利率\s+年([\d.]+)％', content)
                if rate_match:
                    interest_rate = float(rate_match.group(1))
                else:
                    # パターン3: 国庫短期証券（利率なし）
                    interest_rate = 0.0
            
            # 償還期限の抽出（複数パターン対応）
            maturity_date = "不明"
            
            # パターン1: "15　償還期限　令和９年７月１日"（空白あり）
            maturity_match = re.search(r'1[2-9]\s+(?:償還期限|　償還期限)\s+(令和\d+年\d+月\d+日)', content)
            if maturity_match:
                maturity_date = maturity_match.group(1)
            else:
                # パターン2: "12償還期限令和8年2月20日"（空白なし、半角数字）
                maturity_match = re.search(r'1[2-9]\s*償還期限\s*(令和\d+年\d+月\d+日)', content)
                if maturity_match:
                    maturity_date = maturity_match.group(1)
            
            # 発行額の抽出（複数パターン対応）
            face_value = 0.0
            
            # パターン1: "⑴　価格競争入札発行　額面金額で1,977,200,000,000円"
            amount_match = re.search(r'価格競争入札発行\s+額面金額で([\d,]+)円', content)
            if amount_match:
                amount_str = amount_match.group(1).replace(',', '')
                face_value = float(amount_str)
            else:
                # パターン2: "４　発行額　額面金額で163,192,370,000円"（個人向け）
                amount_match = re.search(r'[４４4５5６6]\s+(?:発行額|　発行額)\s+額面金額で([\d,]+)円', content)
                if amount_match:
                    amount_str = amount_match.group(1).replace(',', '')
                    face_value = float(amount_str)
            
            # 発行根拠の抽出（改行をまたぐ、より柔軟に）
            legal_basis = "不明"
            
            # パターン1: 全角・半角数字両対応、終端を柔軟に
            legal_match = re.search(
                r'[２2]\s*発行の根拠法律及びその条項\s*(.+?)(?:\n\s*[３3４4５5６6]|\n\n|振替法)', 
                content, 
                re.DOTALL
            )
            if legal_match:
                legal_basis = legal_match.group(1).strip()
                # 改行を削除、余分な空白を削除
                legal_basis = re.sub(r'\s+', '', legal_basis)
            
            return BondIssuance(
                sub_index=1,
                bond_name=bond_name,
                bond_type=bond_type,
                series_number=series_number,
                interest_rate=interest_rate,
                maturity_date=maturity_date,
                legal_basis=legal_basis,
                face_value_individual=face_value
            )
            
        except Exception as e:
            logger.error(f"単一銘柄の抽出エラー: {e}")
            import traceback
            traceback.print_exc()
            return None