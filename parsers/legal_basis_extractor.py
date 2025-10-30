"""
構造化データ方式による発行根拠法令抽出

アプローチ:
1. 告示文から全ての法律条項を抽出
2. 条項を正規化（法律名、条、項）
3. 条項から発行根拠法令へのマッピング
"""

import re
from typing import List, Tuple, Optional

# ========================================
# Step 1: 条項マッピングテーブル（宣言的）
# ========================================

# (法律略称, 条, 項) → 発行根拠法令
ARTICLE_TO_LEGAL_BASIS = {
    # 特別会計に関する法律
    ("特別会計法", 46, 1): {"basis": "借換債", "category": "借換債", "sub_category": "借換債"},
    ("特別会計法", 47, 1): {"basis": "前倒債", "category": "借換債", "sub_category": "前倒債"},
    ("特別会計法", 62, 1): {"basis": "財投債", "category": "財投債", "sub_category": "財投債"},
    
    # 財政法
    ("財政法", 4, 1): {"basis": "4条債", "category": "4条債", "sub_category": "建設国債"},
    
    # 特例公債法（平成24年法律第101号）
    ("特例公債法", 2, 1): {"basis": "特例債", "category": "特例債", "sub_category": "特例公債"},
    ("特例公債法", 3, 1): {"basis": "年金特例債", "category": "年金特例債", "sub_category": "つなぎ公債"},
    
    # GX推進法（令和5年法律第32号）
    ("GX推進法", 7, 1): {"basis": "GX経済移行債", "category": "GX経済移行債", "sub_category": "つなぎ公債"},
    
    # 復興財源確保法
    ("復興財源確保法", 69, 4): {"basis": "復興債", "category": "復興債", "sub_category": "つなぎ公債"},
}


# ========================================
# Step 2: 法律名の正規化マッピング
# ========================================

LAW_NAME_NORMALIZATION = {
    "特別会計に関する法律": "特別会計法",
    "特別会計法": "特別会計法",
    "平成19年法律第23号": "特別会計法",
    
    "財政法": "財政法",
    
    "財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律": "特例公債法",
    "平成24年法律第101号": "特例公債法",
    
    "脱炭素成長型経済構造への円滑な移行の推進に関する法律": "GX推進法",
    "令和5年法律第32号": "GX推進法",
    "令和５年法律第32号": "GX推進法",
    
    "東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法": "復興財源確保法",
}


# ========================================
# Step 3: 条項抽出パーサー
# ========================================

class LegalArticleParser:
    """法律条項を構造化データに変換するパーサー"""
    
    # 法律名パターン
    LAW_PATTERNS = [
        r'特別会計に関する法律[（\(][^）\)]*[）\)]?',
        r'特別会計法',
        r'平成19年法律第23号',
        r'財政法',
        r'財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律',
        r'平成24年法律第101号',
        r'脱炭素成長型経済構造への円滑な移行の推進に関する法律',
        r'令和[5５]年法律第32号',
        r'東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法',
    ]
    
    # 条項パターン
    ARTICLE_PATTERN = r'第([0-9０-９一二三四五六七八九十百千]+)条第([0-9０-９一二三四五六七八九十]+)項'
    
    @staticmethod
    def normalize_number(num_str: str) -> int:
        """漢数字・全角数字を半角数字に変換"""
        # 全角→半角
        num_str = num_str.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
        
        # 漢数字→数字
        kanji_map = {
            '一': 1, '二': 2, '三': 3, '四': 4, '五': 5,
            '六': 6, '七': 7, '八': 8, '九': 9, '十': 10,
            '百': 100, '千': 1000
        }
        
        if num_str.isdigit():
            return int(num_str)
        
        # 簡易的な漢数字変換（例: 四十六 → 46）
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
        return result if result > 0 else int(num_str) if num_str.isdigit() else 0
    
    def parse_articles(self, text: str) -> List[Tuple[str, int, int]]:
        """
        告示文から全ての法律条項を抽出
        
        Returns:
            List of (法律略称, 条, 項)
        """
        articles = []
        
        # 「発行の根拠法律及びその条項」セクションを抽出
        legal_section_pattern = r'発行の根拠法律及びその条項[^０-９0-9第]*(.{1,500}?)(?:\n[0-9０-９]|$)'
        legal_section_match = re.search(legal_section_pattern, text, re.DOTALL)
        
        if not legal_section_match:
            return articles
        
        legal_section = legal_section_match.group(1)
        
        # 法律名を抽出
        current_law = None
        for law_pattern in self.LAW_PATTERNS:
            match = re.search(law_pattern, legal_section)
            if match:
                law_name = match.group(0)
                # 正規化
                for full_name, short_name in LAW_NAME_NORMALIZATION.items():
                    if full_name in law_name or law_name in full_name:
                        current_law = short_name
                        break
                if current_law:
                    break
        
        if not current_law:
            return articles
        
        # 条項を抽出（複数対応）
        for match in re.finditer(self.ARTICLE_PATTERN, legal_section):
            article_num_str = match.group(1)
            clause_num_str = match.group(2)
            
            article_num = self.normalize_number(article_num_str)
            clause_num = self.normalize_number(clause_num_str)
            
            articles.append((current_law, article_num, clause_num))
        
        return articles


# ========================================
# Step 4: 統合インターフェース
# ========================================

def extract_legal_bases_structured(text: str) -> List[dict]:
    """
    構造化データ方式で発行根拠法令を抽出
    
    Returns:
        List of {basis, category, sub_category, full}
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
# Step 5: テストコード
# ========================================

if __name__ == "__main__":
    # テスト1: 単一条項
    test1 = """
    発行の根拠法律及びその条項
    特別会計に関する法律（平成19年法律第23号）第46条第１項
    """
    result1 = extract_legal_bases_structured(test1)
    print("Test 1:", result1)
    # 期待: [{"basis": "借換債", ...}]
    
    # テスト2: 複数条項（及び）
    test2 = """
    発行の根拠法律及びその条項
    特別会計に関する法律（平成19年法律第23号）第47条第1項及び第62条第1項
    """
    result2 = extract_legal_bases_structured(test2)
    print("Test 2:", result2)
    # 期待: [{"basis": "前倒債", ...}, {"basis": "財投債", ...}]
    
    # テスト3: 財投債のみ
    test3 = """
    発行の根拠法律及びその条項
    特別会計に関する法律第62条第1項
    """
    result3 = extract_legal_bases_structured(test3)
    print("Test 3:", result3)
    # 期待: [{"basis": "財投債", ...}]