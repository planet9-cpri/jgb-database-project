"""
プロジェクトフォルダの構造を簡潔に表示
"""

import os
from pathlib import Path

def show_structure(root_path, max_depth=2, current_depth=0, prefix=""):
    """フォルダ構造を表示（深さ制限付き）"""
    
    if current_depth >= max_depth:
        return
    
    try:
        items = sorted(os.listdir(root_path))
    except PermissionError:
        return
    
    # __pycache__などを除外
    exclude = ['__pycache__', '.git', 'venv', 'env', '.idea']
    items = [item for item in items if item not in exclude]
    
    for i, item in enumerate(items):
        item_path = Path(root_path) / item
        is_last = (i == len(items) - 1)
        
        # 記号
        connector = "└── " if is_last else "├── "
        
        # アイコン
        if item_path.is_dir():
            icon = "📁"
        elif item.endswith('.py'):
            icon = "🐍"
        elif item.endswith('.md'):
            icon = "📄"
        elif item.endswith('.txt'):
            icon = "📄"
        elif item.endswith('.sql'):
            icon = "💾"
        else:
            icon = "📄"
        
        print(f"{prefix}{connector}{icon} {item}")
        
        # サブフォルダを再帰的に表示
        if item_path.is_dir():
            extension = "    " if is_last else "│   "
            show_structure(item_path, max_depth, current_depth + 1, prefix + extension)


def main():
    """メイン実行"""
    project_root = Path(__file__).parent
    
    print("=" * 70)
    print("📁 プロジェクトフォルダ構造")
    print("=" * 70)
    print(f"\nルート: {project_root}")
    print()
    
    print(f"📁 {project_root.name}/")
    show_structure(project_root, max_depth=3)
    
    print()
    print("=" * 70)
    
    # ファイル数をカウント
    py_files = list(project_root.rglob("*.py"))
    md_files = list(project_root.rglob("*.md"))
    sql_files = list(project_root.rglob("*.sql"))
    
    print("\n📊 ファイル統計:")
    print(f"  Pythonファイル: {len(py_files)}件")
    print(f"  Markdownファイル: {len(md_files)}件")
    print(f"  SQLファイル: {len(sql_files)}件")
    
    print()
    print("=" * 70)


if __name__ == '__main__':
    main()