"""
構造化データ方式による発行根拠法令抽出 v3 (本番版)

変更点（v2 → v3）:
- 複数法律の「並びに」「及び」に対応
- 各法律名と条項のペアを正確に抽出
- 法律名ごとにセクションを分割して処理

アプローチ:
1. 告示文から「法律名...第X条第Y項」のペアを抽出
2. 各法律名について、その範囲内の全条項を抽出
3. 条項を正規化（法律名、条、項）
4. 条項から発行根拠法令へのマッピング
"""

import re
from typing import List, Tuple, Optional

# ========================================
# Step 1: 条項マッピングテーブル（宣言的）
# ========================================

ARTICLE_TO_LEGAL_BASIS = {
    # 特別会計に関する法律
    ("特別会計に関する法律", 46, 1): {
        "basis": "借換債",
        "category": "借換債",
        "sub_category": "借換債"
    },
    ("特別会計に関する法律", 47, 1): {
        "basis": "前倒債",
        "category": "借換債",
        "sub_category": "前倒債"
    },
    ("特別会計に関する法律", 62, 1): {
        "basis": "財投債",
        "category": "財投債",
        "sub_category": "財投債"
    },
    
    # 財政法
    ("財政法", 4, 1): {
        "basis": "4条債",
        "category": "4条債",
        "sub_category": "建設国債"
    },
    
    # 財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律（特例公債法）
    ("財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律", 2, 1): {
        "basis": "特例債",
        "category": "特例債",
        "sub_category": "特例公債"
    },
    ("財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律", 3, 1): {
        "basis": "年金特例債",
        "category": "年金特例債",
        "sub_category": "つなぎ公債"
    },
    
    # 脱炭素成長型経済構造への円滑な移行の推進に関する法律（GX推進法）
    ("脱炭素成長型経済構造への円滑な移行の推進に関する法律", 7, 1): {
        "basis": "GX経済移行債",
        "category": "GX経済移行債",
        "sub_category": "つなぎ公債"
    },
    
    # 東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法（復興財源確保法）
    ("東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法", 69, 4): {
        "basis": "復興債",
        "category": "復興債",
        "sub_category": "つなぎ公債"
    },
}

# 法律名の正規化マッピング
LAW_NAME_NORMALIZATION = {
    "特別会計に関する法律": "特別会計に関する法律",
    "財政法": "財政法",
    "財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律": "財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律",
    "脱炭素成長型経済構造への円滑な移行の推進に関する法律": "脱炭素成長型経済構造への円滑な移行の推進に関する法律",
    "東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法": "東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法",
}

# ========================================
# Step 3: 条項抽出パーサー v3
# ========================================

class LegalArticleParser:
    """法律条項を構造化データに変換するパーサー v3"""
    
    # 法律名パターン（正式名称のみ、優先順位順）
    LAW_PATTERNS = [
        '財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律',
        '脱炭素成長型経済構造への円滑な移行の推進に関する法律',
        '東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法',
        '特別会計に関する法律',
        '財政法',
    ]
    
    # 条項パターン
    ARTICLE_PATTERN = r'第([0-9０-９一二三四五六七八九十百千]+)条第([0-9０-９一二三四五六七八九十]+)項'
    
    @staticmethod
    def normalize_number(num_str: str) -> int:
        """漢数字・全角数字を半角数字に変換"""
        # 全角→半角
        num_str = num_str.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
        
        # 既に数字の場合
        if num_str.isdigit():
            return int(num_str)
        
        # 漢数字→数字
        kanji_map = {
            '一': 1, '二': 2, '三': 3, '四': 4, '五': 5,
            '六': 6, '七': 7, '八': 8, '九': 9, '十': 10,
            '百': 100, '千': 1000
        }
        
        result = 0
        temp = 0
        for char in num_str:
            if char in kanji_map:
                val = kanji_map[char]
                if val >= 10:
                    if temp == 0:
                        temp = 1
                    result += temp * val
                    temp = 0
                else:
                    temp = val
        result += temp
        
        return result if result > 0 else 0
    
    def parse_articles(self, text: str) -> List[Tuple[str, int, int]]:
        """
        告示文から全ての法律条項を抽出（v3: 複数法律対応）
        
        Returns:
            List of (法律正式名称, 条, 項)
        """
        articles = []
        
        # 「発行の根拠法律及びその条項」セクションを抽出
        legal_section_pattern = r'発行の根拠法律及びその条項[\s\u3000]+(.{1,1000}?)(?:\n[0-9０-９]|$)'
        legal_section_match = re.search(legal_section_pattern, text, re.DOTALL)
        
        if not legal_section_match:
            return articles
        
        legal_section = legal_section_match.group(1)
        
        # 各法律名について、その後に続く条項を抽出
        for i, law_name in enumerate(self.LAW_PATTERNS):
            # 法律名の正規化
            normalized_law = LAW_NAME_NORMALIZATION.get(law_name, law_name)
            
            # 法律名が存在するか確認
            if law_name not in legal_section:
                continue
            
            # この法律名から次の法律名までのセクションを抽出
            law_start = legal_section.find(law_name)
            
            # 次の法律名を探す
            next_law_start = len(legal_section)
            for j, next_law_name in enumerate(self.LAW_PATTERNS):
                if i != j:  # 自分自身は除外
                    next_pos = legal_section.find(next_law_name, law_start + len(law_name))
                    if next_pos != -1 and next_pos < next_law_start:
                        next_law_start = next_pos
            
            # この法律名のセクションを抽出
            law_section = legal_section[law_start:next_law_start]
            
            # このセクション内のすべての「第X条第Y項」を抽出
            article_matches = list(re.finditer(self.ARTICLE_PATTERN, law_section))
            
            for match in article_matches:
                article_num_str = match.group(1)
                clause_num_str = match.group(2)
                
                article_num = self.normalize_number(article_num_str)
                clause_num = self.normalize_number(clause_num_str)
                
                if article_num > 0 and clause_num > 0:
                    articles.append((normalized_law, article_num, clause_num))
        
        return articles


# ========================================
# Step 4: 統合インターフェース
# ========================================

def extract_legal_bases_structured(text: str) -> List[dict]:
    """
    構造化データ方式で発行根拠法令を抽出（v3）
    
    Args:
        text: 告示文の全文
    
    Returns:
        List of {
            'basis': str,           # 発行根拠（借換債、特例債など）
            'category': str,        # 大分類
            'sub_category': str,    # 詳細分類
            'full': str,            # 条項の完全表記
            'article': tuple        # (法律名, 条, 項)
        }
    """
    parser = LegalArticleParser()
    articles = parser.parse_articles(text)
    
    results = []
    for article in articles:
        legal_info = ARTICLE_TO_LEGAL_BASIS.get(article)
        if legal_info:
            # 重複チェック
            if not any(r['basis'] == legal_info['basis'] for r in results):
                results.append({
                    'basis': legal_info['basis'],
                    'category': legal_info['category'],
                    'sub_category': legal_info['sub_category'],
                    'full': f"{article[0]}第{article[1]}条第{article[2]}項",
                    'article': article
                })
    
    return results


# ========================================
# Step 5: 簡易テストコード（本番では無効化）
# ========================================

if __name__ == "__main__":
    print("=" * 80)
    print("legal_basis_extractor_v3_clean.py - 簡易テスト")
    print("=" * 80)
    
    # テスト: 複数法律（並びに・及び）
    test = """
    発行の根拠法律及びその条項
    財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律（平成24年法律第101号）第３条第１項並びに脱炭素成長型経済構造への円滑な移行の推進に関する法律（令和５年法律第32号）第７条第１項及び特別会計に関する法律（平成19年法律第23号）第46条第１項
    """
    result = extract_legal_bases_structured(test)
    
    print(f"\n抽出件数: {len(result)}")
    for r in result:
        print(f"  - {r['basis']} ({r['category']})")
    
    if len(result) == 3:
        print("\n✅ テスト成功")
    else:
        print(f"\n❌ テスト失敗（期待: 3件、実際: {len(result)}件）")
    
    print("=" * 80)