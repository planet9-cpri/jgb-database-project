"""
構造化データ方式による発行根拠法令抽出 v2 - デバッグ版

デバッグ用の詳細出力を追加
"""

import re
from typing import List, Tuple, Optional

# ========================================
# Step 1: 条項マッピングテーブル（宣言的）
# ========================================

ARTICLE_TO_LEGAL_BASIS = {
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
    ("財政法", 4, 1): {
        "basis": "4条債",
        "category": "4条債",
        "sub_category": "建設国債"
    },
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
    ("脱炭素成長型経済構造への円滑な移行の推進に関する法律", 7, 1): {
        "basis": "GX経済移行債",
        "category": "GX経済移行債",
        "sub_category": "つなぎ公債"
    },
    ("東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法", 69, 4): {
        "basis": "復興債",
        "category": "復興債",
        "sub_category": "つなぎ公債"
    },
}

LAW_NAME_NORMALIZATION = {
    "特別会計に関する法律": "特別会計に関する法律",
    "財政法": "財政法",
    "財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律": "財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律",
    "脱炭素成長型経済構造への円滑な移行の推進に関する法律": "脱炭素成長型経済構造への円滑な移行の推進に関する法律",
    "東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法": "東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法",
}

# ========================================
# Step 3: 条項抽出パーサー
# ========================================

class LegalArticleParser:
    """法律条項を構造化データに変換するパーサー v2 - デバッグ版"""
    
    LAW_PATTERNS = [
        r'財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律',
        r'脱炭素成長型経済構造への円滑な移行の推進に関する法律',
        r'東日本大震災からの復興のための施策を実施するために必要な財源の確保に関する特別措置法',
        r'特別会計に関する法律',
        r'財政法',
    ]
    
    ARTICLE_PATTERN = r'第([0-9０-９一二三四五六七八九十百千]+)条第([0-9０-９一二三四五六七八九十]+)項'
    
    @staticmethod
    def normalize_number(num_str: str) -> int:
        """漢数字・全角数字を半角数字に変換"""
        print(f"    [normalize_number] 入力: '{num_str}'")
        
        # 全角→半角
        num_str = num_str.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
        print(f"    [normalize_number] 全角→半角後: '{num_str}'")
        
        if num_str.isdigit():
            result = int(num_str)
            print(f"    [normalize_number] 数字変換: {result}")
            return result
        
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
        
        print(f"    [normalize_number] 漢数字変換: {result}")
        return result if result > 0 else 0
    
    def parse_articles(self, text: str, debug=True) -> List[Tuple[str, int, int]]:
        """
        告示文から全ての法律条項を抽出（デバッグ版）
        """
        print("\n" + "=" * 60)
        print("[parse_articles] 開始")
        print("=" * 60)
        
        articles = []
        
        # Step 1: セクション抽出
        print("\n[Step 1] セクション抽出")
        # 改行または空白（全角・半角）をスキップし、法律名から抽出開始
        legal_section_pattern = r'発行の根拠法律及びその条項[\s\u3000]+(.{1,1000}?)(?:\n[0-9０-９]|$)'
        legal_section_match = re.search(legal_section_pattern, text, re.DOTALL)
        
        if not legal_section_match:
            print("  ❌ セクションが見つかりませんでした")
            return articles
        
        legal_section = legal_section_match.group(1)
        print(f"  ✅ セクション抽出成功")
        print(f"  セクション内容: '{legal_section[:200]}...'")
        
        # Step 2: 法律名抽出
        print("\n[Step 2] 法律名抽出")
        current_law = None
        for i, law_pattern in enumerate(self.LAW_PATTERNS):
            print(f"  パターン{i+1}: {law_pattern}")
            match = re.search(law_pattern, legal_section)
            if match:
                law_name = match.group(0)
                print(f"    ✅ マッチ: '{law_name}'")
                if law_name in LAW_NAME_NORMALIZATION:
                    current_law = LAW_NAME_NORMALIZATION[law_name]
                    print(f"    ✅ 正規化後: '{current_law}'")
                    break
            else:
                print(f"    ❌ マッチせず")
        
        if not current_law:
            print("  ❌ 法律名が見つかりませんでした")
            return articles
        
        print(f"\n  最終的な法律名: '{current_law}'")
        
        # Step 3: 条項抽出
        print("\n[Step 3] 条項抽出")
        print(f"  条項パターン: {self.ARTICLE_PATTERN}")
        
        matches = list(re.finditer(self.ARTICLE_PATTERN, legal_section))
        print(f"  マッチ数: {len(matches)}")
        
        for i, match in enumerate(matches):
            print(f"\n  マッチ{i+1}:")
            article_num_str = match.group(1)
            clause_num_str = match.group(2)
            print(f"    条: '{article_num_str}'")
            print(f"    項: '{clause_num_str}'")
            
            article_num = self.normalize_number(article_num_str)
            clause_num = self.normalize_number(clause_num_str)
            
            if article_num > 0 and clause_num > 0:
                articles.append((current_law, article_num, clause_num))
                print(f"    ✅ 追加: ({current_law}, {article_num}, {clause_num})")
            else:
                print(f"    ❌ 無効な数値")
        
        print("\n" + "=" * 60)
        print(f"[parse_articles] 終了 - 抽出数: {len(articles)}")
        print("=" * 60)
        
        return articles


def extract_legal_bases_structured(text: str, debug=True) -> List[dict]:
    """構造化データ方式で発行根拠法令を抽出（デバッグ版）"""
    parser = LegalArticleParser()
    articles = parser.parse_articles(text, debug=debug)
    
    print("\n" + "=" * 60)
    print("[extract_legal_bases_structured] マッピング処理")
    print("=" * 60)
    
    results = []
    for article in articles:
        print(f"\n  条項: {article}")
        legal_info = ARTICLE_TO_LEGAL_BASIS.get(article)
        if legal_info:
            print(f"    ✅ マッピング成功: {legal_info['basis']}")
            if not any(r['basis'] == legal_info['basis'] for r in results):
                results.append({
                    'basis': legal_info['basis'],
                    'category': legal_info['category'],
                    'sub_category': legal_info['sub_category'],
                    'full': f"{article[0]}第{article[1]}条第{article[2]}項",
                    'article': article
                })
        else:
            print(f"    ❌ マッピング失敗（ARTICLE_TO_LEGAL_BASISに存在しません）")
    
    print(f"\n  最終結果数: {len(results)}")
    print("=" * 60)
    
    return results


# ========================================
# Step 5: テストコード
# ========================================

if __name__ == "__main__":
    print("=" * 80)
    print("legal_basis_extractor_v2_debug.py - デバッグテスト実行")
    print("=" * 80)
    
    # テスト1: 借換債（特別会計に関する法律 第46条第1項）
    print("\n\n" + "█" * 80)
    print("【テスト1】借換債")
    print("█" * 80)
    test1 = """
    発行の根拠法律及びその条項
    特別会計に関する法律（平成19年法律第23号）第46条第１項
    """
    
    print(f"\n入力テキスト:")
    print(f"'{test1}'")
    print(f"\n入力テキスト（repr）:")
    print(repr(test1))
    
    result1 = extract_legal_bases_structured(test1)
    
    print("\n" + "=" * 80)
    print("【結果】")
    print(f"結果: {result1}")
    print(f"期待: 借換債")
    print("=" * 80)