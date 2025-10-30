"""
KanpoParserの動作確認スクリプト

使用方法:
    python scripts/test_kanpo_parser.py
"""

import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from parsers.kanpo_parser import KanpoParser
from parsers.table_parser import TableParser
import json

DATA_DIR = r"G:\マイドライブ\JGBデータ\2023"

def test_parser():
    """パーサーのテスト"""
    print("="*60)
    print("🔍 KanpoParser 動作確認")
    print("="*60)
    
    # パーサーの初期化
    kanpo_parser = KanpoParser()
    table_parser = TableParser()
    
    # テスト用ファイルを取得（最初の3ファイル）
    data_path = Path(DATA_DIR)
    files = sorted(data_path.glob("*.txt"))[:3]
    
    for idx, file_path in enumerate(files, 1):
        print(f"\n{'='*60}")
        print(f"📄 ファイル {idx}: {file_path.name}")
        print(f"{'='*60}")
        
        try:
            # 解析
            result = kanpo_parser.parse_file(str(file_path))
            
            if not result:
                print("❌ 解析失敗")
                continue
            
            # 告示情報の表示
            announcement_info = result.get('announcement_info', {})
            print("\n【告示情報】")
            print(f"  kanpo_date: {announcement_info.get('kanpo_date', 'なし')}")
            print(f"  announcement_number: {announcement_info.get('announcement_number', 'なし')}")
            print(f"  gazette_issue_number: {announcement_info.get('gazette_issue_number', 'なし')}")
            print(f"  title: {announcement_info.get('title', 'なし')[:50]}...")
            print(f"  announcement_type: {announcement_info.get('announcement_type', 'なし')}")
            
            # 別表情報の表示
            tables = result.get('tables', [])
            print(f"\n【別表】 {len(tables)} 個")
            
            # 銘柄情報の抽出
            bond_issuances = []
            
            if tables:
                for table_idx, table in enumerate(tables, 1):
                    print(f"\n  別表 {table_idx}:")
                    table_content = table.get('content', '')
                    common_legal_basis = table.get('common_legal_basis')
                    
                    print(f"    共通法令根拠: {common_legal_basis or 'なし'}")
                    print(f"    内容の長さ: {len(table_content)} 文字")
                    
                    # TableParserで解析
                    issuances = table_parser.parse_table(
                        table_content, 
                        common_legal_basis
                    )
                    bond_issuances.extend(issuances)
                    print(f"    抽出された銘柄数: {len(issuances)}")
            else:
                print("\n  別表なし - 単一銘柄の告示")
                
                # 単一銘柄の抽出
                single_issuance = table_parser.extract_bond_info_from_single(
                    result.get('content', '')
                )
                
                if single_issuance:
                    bond_issuances.append(single_issuance)
                    print(f"  単一銘柄抽出: 成功")
                else:
                    print(f"  単一銘柄抽出: 失敗")
            
            # 銘柄情報の詳細表示
            print(f"\n【銘柄情報】 合計 {len(bond_issuances)} 件")
            for bond_idx, issuance in enumerate(bond_issuances[:3], 1):  # 最初の3件のみ
                print(f"\n  銘柄 {bond_idx}:")
                print(f"    債券種類: {getattr(issuance, 'bond_type', 'なし')}")
                print(f"    発行日: {getattr(issuance, 'issuance_date', 'なし')}")
                print(f"    償還日: {getattr(issuance, 'maturity_date', 'なし')}")
                print(f"    利率: {getattr(issuance, 'interest_rate', 'なし')}")
                print(f"    発行価額: {getattr(issuance, 'issue_price', 'なし')}")
                print(f"    発行額: {getattr(issuance, 'issue_amount', 'なし')}")
                print(f"    法令根拠: {getattr(issuance, 'legal_basis', 'なし')}")
            
            if len(bond_issuances) > 3:
                print(f"\n  ... 他 {len(bond_issuances) - 3} 件")
            
        except Exception as e:
            print(f"\n❌ エラー: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "="*60)
    print("✅ テスト完了")
    print("="*60)


if __name__ == "__main__":
    test_parser()