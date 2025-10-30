"""
漢数字変換のテストスクリプト

使用方法:
    python scripts/test_kanji_conversion.py
"""

def convert_kanji_to_number(kanji_str: str) -> str:
    """漢数字を数字に変換
    
    例:
    - 百二十一 → 121
    - 二十三 → 23
    - 五 → 5
    - 三百 → 300
    """
    kanji_to_digit = {
        '一': 1, '二': 2, '三': 3, '四': 4, '五': 5,
        '六': 6, '七': 7, '八': 8, '九': 9,
    }
    
    total = 0
    current = 0
    
    i = 0
    while i < len(kanji_str):
        char = kanji_str[i]
        
        if char in kanji_to_digit:
            current = kanji_to_digit[char]
        elif char == '十':
            if current == 0:
                current = 10
            else:
                current *= 10
        elif char == '百':
            if current == 0:
                current = 100
            else:
                current *= 100
        elif char == '千':
            if current == 0:
                current = 1000
            else:
                current *= 1000
        elif char == '万':
            if current == 0:
                current = 10000
            else:
                current *= 10000
        else:
            # 未知の文字はスキップ
            i += 1
            continue
        
        # 次の文字を先読み
        if i + 1 < len(kanji_str):
            next_char = kanji_str[i + 1]
            # 次が位取りの文字でなければ、現在値を加算
            if next_char not in ['十', '百', '千', '万']:
                total += current
                current = 0
        else:
            # 最後の文字
            total += current
            current = 0
        
        i += 1
    
    # 残りがあれば加算
    if current > 0:
        total += current
    
    return str(total) if total > 0 else kanji_str


def test_conversion():
    """変換のテスト"""
    test_cases = [
        ('百二十一', '121'),  # 実際のファイル名から
        ('百二十三', '123'),
        ('百二十五', '125'),
        ('百二十九', '129'),
        ('百二十二', '122'),
        ('百二十八', '128'),
        ('百二十七', '127'),
        ('百三十一', '131'),
        ('百三十二', '132'),
        ('百三十', '130'),
        ('二十三', '23'),
        ('五', '5'),
        ('十', '10'),
        ('三百', '300'),
        ('千', '1000'),
        ('一', '1'),
        ('九', '9'),
    ]
    
    print("="*60)
    print("🧪 漢数字変換テスト")
    print("="*60)
    
    success_count = 0
    fail_count = 0
    
    for kanji, expected in test_cases:
        result = convert_kanji_to_number(kanji)
        status = "✅" if result == expected else "❌"
        
        if result == expected:
            success_count += 1
        else:
            fail_count += 1
        
        print(f"{status} {kanji:10s} → {result:5s} (期待: {expected})")
    
    print("\n" + "="*60)
    print(f"結果: ✅ {success_count} 成功 / ❌ {fail_count} 失敗")
    print("="*60)


if __name__ == "__main__":
    test_conversion()