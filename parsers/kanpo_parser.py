"""
KanpoParser - 官報ファイルから国債発行告示情報を抽出

Phase 1: 2023年度の官報データ解析
Author: Person B (Parser Implementation)
"""

import re
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import logging

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KanpoParser:
    """
    官報テキストファイルから国債発行告示情報を抽出するパーサー
    
    処理フロー:
    1. ファイル読み込み（UTF-8）
    2. ファイル名から情報を抽出
    3. 告示情報の抽出（告示番号、日付、タイトル等）
    4. 別表（テーブル）の抽出
    5. 構造化データとして返却
    """
    
    # 正規表現パターン
    PATTERNS = {
        'kanpo_number': r'(?:号外)?第\d+号',
        'announcement_number': r'(財務省|総務省)(?:告示)?第.+?号',
        'date': r'令和(\d+)年(\d+)月(\d+)日',
        'table_start': r'別\s*表(?:第[一二三四五六七八九十]+)?',
        'amount': r'[\d,]+(?:億|万)?円',
    }
    
    def __init__(self, encoding: str = 'utf-8'):
        """パーサーの初期化"""
        self.encoding = encoding
        self.stats = {
            'files_processed': 0,
            'announcements_found': 0,
            'tables_extracted': 0,
            'errors': 0
        }
    
    def parse_filename(self, filename: str) -> dict:
        """
        ファイル名から情報を抽出
        例: 20230403_令和5年5月9日付（財務省第百二十一号）.txt
        """
        try:
            # 国債発行日（YYYYMMDD）
            issue_date_match = re.match(r'(\d{8})_', filename)
            if issue_date_match:
                issue_date = datetime.strptime(issue_date_match.group(1), '%Y%m%d').date()
                issue_date_str = str(issue_date)
            else:
                issue_date_str = None
            
            # 告示日付（和暦）
            announce_date_match = re.search(r'令和(\d+)年(\d+)月(\d+)日付', filename)
            if announce_date_match:
                year = int(announce_date_match.group(1)) + 2018
                month = int(announce_date_match.group(2))
                day = int(announce_date_match.group(3))
                announce_date = f"{year}-{month:02d}-{day:02d}"
            else:
                announce_date = None
            
            # 告示番号（修正版 - 財務省/総務省のみ）
            # パターン1: （財務省第XXX号）形式
            announcement_number_match = re.search(r'（(財務省|総務省)第.+?号）', filename)
            if announcement_number_match:
                announcement_number = announcement_number_match.group(1)
            else:
                # パターン2: 他の括弧内テキストは無視
                announcement_number = None
            
            return {
                'issue_date': issue_date_str,
                'announce_date': announce_date,
                'announcement_number': announcement_number
            }
        except Exception as e:
            logger.error(f"ファイル名解析エラー: {filename} - {str(e)}")
            return {
                'issue_date': None,
                'announce_date': None,
                'announcement_number': None
            }
    
    def parse_file(self, filepath: str) -> Optional[Dict]:
        """
        官報ファイルを解析してデータを抽出
        
        Args:
            filepath: 官報テキストファイルのパス
            
        Returns:
            {
                'source_file': str,
                'issue_date': str,
                'announcement_number': str,
                'announcement_date': str,
                'ministry': str,
                'title': str,
                'content': str,
                'tables': List[Dict],
                'parsed_at': str
            }
        """
        try:
            filepath = Path(filepath)
            
            if not filepath.exists():
                logger.error(f"ファイルが見つかりません: {filepath}")
                return None
            
            # ファイル読み込み
            with open(filepath, 'r', encoding=self.encoding) as f:
                text = f.read()
            
            logger.info(f"ファイル読み込み完了: {filepath.name}")
            
            # ファイル名から情報を抽出
            filename_info = self.parse_filename(filepath.name)
            
            # 告示情報の抽出
            announcement_info = self.extract_announcement_info(text)
            
            # ファイル名と本文の情報をマージ（ファイル名を優先）
            announcement_number = filename_info['announcement_number'] or announcement_info['announcement_number']
            announcement_date = filename_info['announce_date'] or announcement_info['kanpo_date']
            
            # 別表の抽出
            tables = self.extract_tables(text)
            
            # 統計更新
            self.stats['files_processed'] += 1
            if announcement_number:
                self.stats['announcements_found'] += 1
            self.stats['tables_extracted'] += len(tables)
            
            # フラットな構造で返却
            return {
                'source_file': filepath.name,
                'issue_date': filename_info['issue_date'],
                'announcement_number': announcement_number,
                'announcement_date': announcement_date,
                'ministry': announcement_info['ministry'],
                'title': announcement_info.get('title'),
                'kanpo_number': announcement_info.get('kanpo_number'),
                'content': text,
                'tables': tables,
                'parsed_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"ファイル解析エラー: {filepath} - {str(e)}")
            self.stats['errors'] += 1
            return None
    
    def extract_announcement_info(self, text: str) -> Dict:
        """告示情報を抽出"""
        info = {
            'kanpo_number': None,
            'kanpo_date': None,
            'announcement_number': None,
            'ministry': None,
            'title': None,
        }
        
        # 官報番号の抽出
        kanpo_match = re.search(self.PATTERNS['kanpo_number'], text)
        if kanpo_match:
            info['kanpo_number'] = kanpo_match.group(0)
        
        # 告示番号の抽出
        ann_match = re.search(self.PATTERNS['announcement_number'], text)
        if ann_match:
            info['announcement_number'] = ann_match.group(0)
            # 省庁名も抽出
            announcement_text = ann_match.group(0)
            if '財務省' in announcement_text:
                info['ministry'] = '財務省'
            elif '総務省' in announcement_text:
                info['ministry'] = '総務省'
        
        # 日付の抽出
        date_match = re.search(self.PATTERNS['date'], text)
        if date_match:
            year = int(date_match.group(1)) + 2018
            month = int(date_match.group(2))
            day = int(date_match.group(3))
            info['kanpo_date'] = f"{year}-{month:02d}-{day:02d}"
        
        # タイトルの抽出
        if ann_match:
            title_start = ann_match.end()
            title_end = text.find('\n', title_start)
            if title_end > title_start:
                info['title'] = text[title_start:title_end].strip()
        
        return info
    
    def extract_tables(self, text: str) -> List[Dict]:
        """別表を抽出"""
        tables = []
        
        table_starts = [
            match.start() 
            for match in re.finditer(self.PATTERNS['table_start'], text)
        ]
        
        if not table_starts:
            logger.info("別表なし（単一銘柄の告示）")
            return tables
        
        for i, start in enumerate(table_starts):
            end = table_starts[i + 1] if i + 1 < len(table_starts) else len(text)
            table_text = text[start:end]
            
            title_match = re.match(r'別\s*表(?:第[一二三四五六七八九十]+)?', table_text)
            table_title = title_match.group(0) if title_match else f"別表{i+1}"
            
            tables.append({
                'table_number': i + 1,
                'table_title': table_title,
                'table_text': table_text.strip(),
                'start_position': start,
                'end_position': end
            })
        
        logger.info(f"別表を{len(tables)}個抽出しました")
        return tables
    
    def parse_directory(self, directory: str, pattern: str = "*.txt") -> List[Dict]:
        """ディレクトリ内の複数ファイルを一括解析"""
        directory = Path(directory)
        
        if not directory.exists():
            logger.error(f"ディレクトリが見つかりません: {directory}")
            return []
        
        files = sorted(directory.glob(pattern))
        logger.info(f"{len(files)}個のファイルを処理します")
        
        results = []
        for filepath in files:
            data = self.parse_file(str(filepath))
            if data:
                results.append(data)
        
        logger.info(f"処理完了: {self.stats}")
        return results
    
    def get_stats(self) -> Dict:
        """パース統計を取得"""
        return self.stats.copy()
    
    def reset_stats(self):
        """統計情報をリセット"""
        self.stats = {
            'files_processed': 0,
            'announcements_found': 0,
            'tables_extracted': 0,
            'errors': 0
        }


if __name__ == "__main__":
    parser = KanpoParser()
    
    # 単一ファイルテスト
    test_file = r"G:\マイドライブ\JGBデータ\2023\20230403_令和5年5月9日付（財務省第百二十一号）.txt"
    
    result = parser.parse_file(test_file)
    
    if result:
        print("\n✅ 解析成功！")
        print(f"ファイル名: {result['source_file']}")
        print(f"国債発行日: {result['issue_date']}")
        print(f"告示番号: {result['announcement_number']}")
        print(f"別表数: {len(result['tables'])}")