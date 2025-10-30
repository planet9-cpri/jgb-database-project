"""
官報ファイルの内容を確認するスクリプト

使用方法:
    python scripts/check_file_content.py
"""

from pathlib import Path

DATA_DIR = r"G:\マイドライブ\JGBデータ\2023"

def check_files():
    """ファイル内容を確認"""
    print("="*60)
    print("📄 官報ファイル内容の確認")
    print("="*60)
    
    data_path = Path(DATA_DIR)
    files = sorted(data_path.glob("*.txt"))[:2]  # 最初の2ファイル
    
    for idx, file_path in enumerate(files, 1):
        print(f"\n{'='*60}")
        print(f"ファイル {idx}: {file_path.name}")
        print(f"{'='*60}\n")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 最初の2000文字を表示
            print(content[:2000])
            print("\n...")
            print(f"\n（ファイル全体: {len(content)} 文字）")
            
        except Exception as e:
            print(f"❌ エラー: {e}")
    
    print("\n" + "="*60)
    print("✅ 確認完了")
    print("="*60)


if __name__ == "__main__":
    check_files()