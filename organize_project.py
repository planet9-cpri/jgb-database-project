"""
プロジェクトフォルダを整理するスクリプト
Day 5 Phase 1: フォルダ整理
"""

import os
import shutil
from pathlib import Path
from datetime import datetime

class ProjectOrganizer:
    """プロジェクトフォルダ整理クラス"""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.moves = []  # 移動履歴
        
    def create_directory_structure(self):
        """必要なディレクトリ構造を作成"""
        print("=" * 70)
        print("📁 ディレクトリ構造の作成")
        print("=" * 70)
        
        directories = [
            # scriptsのサブフォルダ
            "scripts/01_data_ingestion",
            "scripts/02_data_correction",
            "scripts/03_data_validation",
            "scripts/04_utilities",
            # その他
            "archive/parsers",
            "archive/old_scripts",
            "output/temp",
        ]
        
        for dir_path in directories:
            full_path = self.project_root / dir_path
            if not full_path.exists():
                full_path.mkdir(parents=True, exist_ok=True)
                print(f"✅ 作成: {dir_path}")
            else:
                print(f"   既存: {dir_path}")
    
    def move_file(self, source: str, dest_dir: str, reason: str = ""):
        """ファイルを移動"""
        source_path = self.project_root / source
        dest_path = self.project_root / dest_dir / Path(source).name
        
        if not source_path.exists():
            print(f"⚠️ スキップ: {source} (存在しません)")
            return
        
        if dest_path.exists():
            print(f"⚠️ スキップ: {source} (移動先に同名ファイルが存在)")
            return
        
        try:
            shutil.move(str(source_path), str(dest_path))
            self.moves.append({
                'source': source,
                'dest': str(dest_path.relative_to(self.project_root)),
                'reason': reason
            })
            print(f"✅ 移動: {source} → {dest_dir}")
        except Exception as e:
            print(f"❌ エラー: {source} - {e}")
    
    def organize_day4_files(self):
        """Day 4で作成したファイルを整理"""
        print("\n" + "=" * 70)
        print("📦 Day 4ファイルの整理")
        print("=" * 70)
        
        # Day 4のスクリプト
        self.move_file(
            "fix_bond_master_ids.py",
            "scripts/02_data_correction",
            "Day 4で作成したBOND_001修正スクリプト"
        )
        
        # Day 4のテストスクリプト
        self.move_file(
            "test_single_update.py",
            "tests",
            "Day 4で作成したテストスクリプト"
        )
        
        self.move_file(
            "test_tanki_extraction.py",
            "tests",
            "Day 4で作成したテストスクリプト"
        )
    
    def organize_root_files(self):
        """ルートディレクトリのファイルを整理"""
        print("\n" + "=" * 70)
        print("📦 ルートファイルの整理")
        print("=" * 70)
        
        # データ投入スクリプト
        data_ingestion_files = [
            "batch_issue_extractor.py",
            "upload_issues_to_bigquery.py",
        ]
        
        for file in data_ingestion_files:
            self.move_file(
                file,
                "scripts/01_data_ingestion",
                "データ投入スクリプト"
            )
        
        # テストファイル
        test_files = [
            "test_notice_parser_integration.py",
        ]
        
        for file in test_files:
            self.move_file(
                file,
                "tests",
                "テストスクリプト"
            )
        
        # 一時ファイル
        temp_files = [
            "extraction_results.json",
        ]
        
        for file in temp_files:
            self.move_file(
                file,
                "output/temp",
                "一時ファイル"
            )
    
    def organize_scripts_folder(self):
        """scriptsフォルダを分類"""
        print("\n" + "=" * 70)
        print("📦 scriptsフォルダの分類")
        print("=" * 70)
        
        # データ投入
        data_ingestion = [
            "load_issuance_data.py",
            "load_masters.py",
        ]
        
        for file in data_ingestion:
            self.move_file(
                f"scripts/{file}",
                "scripts/01_data_ingestion",
                "データ投入"
            )
        
        # データ検証
        validation = [
            "check_table_schemas.py",
        ]
        
        for file in validation:
            self.move_file(
                f"scripts/{file}",
                "scripts/03_data_validation",
                "データ検証"
            )
        
        # ユーティリティ
        utilities = [
            "check_bq_quota.py",
            "test_bigquery_connection.py",
            "debug_data_inspection.py",
            "check_file_content.py",
            "execute_table_creation.py",
            "recreate_tables.py",
            "test_kanji_conversion.py",
            "test_kanpo_parser.py",
        ]
        
        for file in utilities:
            self.move_file(
                f"scripts/{file}",
                "scripts/04_utilities",
                "ユーティリティ"
            )
    
    def archive_backup_files(self):
        """バックアップファイルをアーカイブ"""
        print("\n" + "=" * 70)
        print("📦 バックアップファイルのアーカイブ")
        print("=" * 70)
        
        backup_files = [
            "parsers/table_parser.py.backup",
        ]
        
        for file in backup_files:
            self.move_file(
                file,
                "archive/parsers",
                "バックアップファイル"
            )
        
        # oldフォルダ全体を移動
        old_folder = self.project_root / "parsers" / "old"
        if old_folder.exists():
            dest_folder = self.project_root / "archive" / "parsers" / "old"
            if not dest_folder.exists():
                try:
                    shutil.move(str(old_folder), str(dest_folder))
                    print(f"✅ 移動: parsers/old → archive/parsers/old")
                except Exception as e:
                    print(f"❌ エラー: parsers/old - {e}")
    
    def delete_unnecessary_files(self):
        """不要なファイルを削除"""
        print("\n" + "=" * 70)
        print("🗑️ 不要なファイルの削除")
        print("=" * 70)
        
        files_to_delete = [
            "index.tx",  # 用途不明
        ]
        
        for file in files_to_delete:
            file_path = self.project_root / file
            if file_path.exists():
                response = input(f"❓ {file} を削除しますか？ (y/n): ")
                if response.lower() == 'y':
                    try:
                        file_path.unlink()
                        print(f"✅ 削除: {file}")
                    except Exception as e:
                        print(f"❌ エラー: {file} - {e}")
                else:
                    print(f"   スキップ: {file}")
    
    def create_init_files(self):
        """__init__.pyを作成"""
        print("\n" + "=" * 70)
        print("📝 __init__.pyの作成")
        print("=" * 70)
        
        dirs_need_init = [
            "scripts/01_data_ingestion",
            "scripts/02_data_correction",
            "scripts/03_data_validation",
            "scripts/04_utilities",
        ]
        
        for dir_path in dirs_need_init:
            init_file = self.project_root / dir_path / "__init__.py"
            if not init_file.exists():
                init_file.write_text("# Auto-generated __init__.py\n")
                print(f"✅ 作成: {dir_path}/__init__.py")
            else:
                print(f"   既存: {dir_path}/__init__.py")
    
    def generate_move_report(self):
        """移動履歴のレポートを生成"""
        print("\n" + "=" * 70)
        print("📋 移動履歴レポート")
        print("=" * 70)
        
        if not self.moves:
            print("移動したファイルはありません")
            return
        
        report_path = self.project_root / "logs" / f"organize_report_{self.timestamp}.txt"
        report_path.parent.mkdir(exist_ok=True)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(f"プロジェクト整理レポート\n")
            f.write(f"実行日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n")
            f.write("=" * 70 + "\n\n")
            
            for i, move in enumerate(self.moves, 1):
                f.write(f"{i}. {move['source']}\n")
                f.write(f"   → {move['dest']}\n")
                if move['reason']:
                    f.write(f"   理由: {move['reason']}\n")
                f.write("\n")
        
        print(f"✅ レポート保存: {report_path.relative_to(self.project_root)}")
        print(f"   移動ファイル数: {len(self.moves)}件")
    
    def run(self):
        """整理を実行"""
        print("\n" + "=" * 70)
        print("🚀 プロジェクトフォルダ整理開始")
        print("=" * 70)
        print(f"プロジェクトルート: {self.project_root}")
        print()
        
        response = input("整理を開始しますか？ (yes/no): ")
        if response.lower() != 'yes':
            print("キャンセルしました")
            return
        
        # 実行
        self.create_directory_structure()
        self.organize_day4_files()
        self.organize_root_files()
        self.organize_scripts_folder()
        self.archive_backup_files()
        self.delete_unnecessary_files()
        self.create_init_files()
        self.generate_move_report()
        
        print("\n" + "=" * 70)
        print("🎉 整理完了！")
        print("=" * 70)
        print()
        print("次のステップ:")
        print("1. python check_structure.py で新しい構造を確認")
        print("2. 既存スクリプトが正常動作するかテスト")
        print("3. Phase 2（ドキュメント作成）へ進む")


def main():
    """メイン実行"""
    project_root = Path(__file__).parent
    organizer = ProjectOrganizer(project_root)
    organizer.run()


if __name__ == '__main__':
    main()