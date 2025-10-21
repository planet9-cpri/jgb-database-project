# tests/inspect_kanpo_file.py
"""
官報ファイルの内容を詳しく確認するスクリプト
"""

from pathlib import Path

def inspect_file():
    """官報ファイルの内容を詳しく確認"""
    
    # 最初のファイルを選択
    data_dir = Path(r"G:\マイドライブ\JGBデータ\2023")
    files = list(data_dir.glob("*.txt"))
    
    if not files:
        print("❌ ファイルが見つかりません")
        return
    
    test_file = files[0]
    
    print("="*60)
    print(f"📄 ファイル名: {test_file.name}")
    print("="*60)
    
    # ファイル名の解析
    filename = test_file.stem  # 拡張子を除いたファイル名
    print(f"\n🔍 ファイル名の構造:")
    print(f"完全なファイル名: {filename}")
    
    # アンダースコアで分割
    parts = filename.split('_')
    if len(parts) >= 2:
        print(f"発行日: {parts[0]}")
        print(f"告示情報: {parts[1]}")
    
    # ファイルの内容を読み込み
    print("\n📖 ファイルの内容:")
    print("="*60)
    
    try:
        with open(test_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print(f"文字数: {len(content)}文字")
        print(f"行数: {len(content.splitlines())}行")
        
        print("\n最初の50行:")
        print("-"*60)
        lines = content.splitlines()
        for i, line in enumerate(lines[:50], 1):
            print(f"{i:3d}: {line}")
        
        print("\n" + "="*60)
        print("🔍 重要なキーワードを検索:")
        print("="*60)
        
        # キーワード検索
        keywords = ['告示', '財務省', '別表', '国債', '発行']
        for keyword in keywords:
            count = content.count(keyword)
            print(f"'{keyword}': {count}回出現")
            if count > 0:
                # 最初の出現位置を表示
                index = content.find(keyword)
                context = content[max(0, index-50):index+100]
                print(f"  最初の出現箇所: ...{context}...")
        
    except Exception as e:
        print(f"❌ エラー: {e}")

if __name__ == "__main__":
    inspect_file()