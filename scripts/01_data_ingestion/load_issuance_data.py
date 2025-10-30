"""
発行データをBigQueryに投入するスクリプト

使用方法:
    python scripts/load_issuance_data.py --limit 10
    python scripts/load_issuance_data.py  # 全ファイル処理
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from google.cloud import bigquery
import pandas as pd
import re

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from parsers.kanpo_parser import KanpoParser
from parsers.table_parser import TableParser

# 設定
PROJECT_ID = "jgb2023"
DATASET_ID = "20251019"
DATA_DIR = r"G:\マイドライブ\JGBデータ\2023"
SERVICE_ACCOUNT_KEY = r"C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json"


class IssuanceDataLoader:
    """発行データをBigQueryに投入するクラス"""
    
    def __init__(self, project_id: str, dataset_id: str, service_account_key: str):
        """初期化"""
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id
        self.kanpo_parser = KanpoParser()
        self.table_parser = TableParser()
        
        self.stats = {
            'files_processed': 0,
            'files_failed': 0,
            'announcements_inserted': 0,
            'issuances_inserted': 0,
            'legal_basis_inserted': 0,
            'errors': []
        }
    
    def get_kanpo_files(self, data_dir: str, limit: Optional[int] = None) -> List[Path]:
        """官報ファイルの一覧を取得"""
        data_path = Path(data_dir)
        if not data_path.exists():
            raise FileNotFoundError(f"データディレクトリが見つかりません: {data_dir}")
        
        files = sorted(data_path.glob("*.txt"))
        if limit:
            files = files[:limit]
        
        print(f"📂 対象ファイル数: {len(files)}")
        return files
    
    def parse_kanpo_file(self, file_path: Path) -> Optional[Dict]:
        """官報ファイルを解析"""
        try:
            result = self.kanpo_parser.parse_file(str(file_path))
            if not result:
                print(f"⚠️ 解析失敗: {file_path.name}")
                return None
            
            announcement_info = result.get('announcement_info', {})
            
            if not announcement_info.get('kanpo_date'):
                announcement_info = self._extract_info_from_filename(file_path.name)
                print(f"  ℹ️ ファイル名から告示情報を抽出")
            
            tables = result.get('tables', [])
            
            print(f"\n📄 {file_path.name}")
            print(f"  告示日: {announcement_info.get('kanpo_date', 'なし')}")
            print(f"  告示番号: {announcement_info.get('announcement_number', 'なし')}")
            print(f"  別表数: {len(tables)}")
            
            bond_issuances = []
            
            if tables:
                for table in tables:
                    table_content = table.get('content', '')
                    common_legal_basis = table.get('common_legal_basis')
                    issuances = self.table_parser.parse_table(table_content, common_legal_basis)
                    bond_issuances.extend(issuances)
            else:
                single_issuance = self.table_parser.extract_bond_info_from_single(result.get('content', ''))
                if single_issuance:
                    bond_issuances.append(single_issuance)
            
            print(f"  銘柄数: {len(bond_issuances)}")
            
            return {
                'announcement': announcement_info,
                'issuances': bond_issuances,
                'source_file': file_path.name
            }
            
        except Exception as e:
            print(f"❌ 解析エラー: {file_path.name} - {e}")
            self.stats['errors'].append({'file': file_path.name, 'error': str(e)})
            return None
    
    def _extract_info_from_filename(self, filename: str) -> Dict:
        """ファイル名から告示情報を抽出"""
        info = {}
        
        date_match = re.match(r'(\d{8})_', filename)
        if date_match:
            date_str = date_match.group(1)
            info['kanpo_date'] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        
        number_match = re.search(r'財務省第(.+?)号', filename)
        if number_match:
            number_str = number_match.group(1)
            number = self._convert_kanji_to_number(number_str)
            info['announcement_number'] = f"第{number}号"
        
        gazette_date_match = re.search(r'(令和\d+年\d+月\d+日付)', filename)
        if gazette_date_match:
            info['gazette_issue_number'] = gazette_date_match.group(1)
        
        info['title'] = ''
        info['announcement_type'] = '発行告示'
        
        return info
    
    def _convert_kanji_to_number(self, kanji_str: str) -> str:
        """漢数字を数字に変換"""
        kanji_to_digit = {'一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9}
        
        total = 0
        current = 0
        
        i = 0
        while i < len(kanji_str):
            char = kanji_str[i]
            
            if char in kanji_to_digit:
                current = kanji_to_digit[char]
            elif char == '十':
                current = 10 if current == 0 else current * 10
            elif char == '百':
                current = 100 if current == 0 else current * 100
            elif char == '千':
                current = 1000 if current == 0 else current * 1000
            elif char == '万':
                current = 10000 if current == 0 else current * 10000
            else:
                i += 1
                continue
            
            if i + 1 < len(kanji_str):
                next_char = kanji_str[i + 1]
                if next_char not in ['十', '百', '千', '万']:
                    total += current
                    current = 0
            else:
                total += current
                current = 0
            
            i += 1
        
        if current > 0:
            total += current
        
        return str(total) if total > 0 else kanji_str
    
    def _convert_wareki_to_date(self, wareki_str: str) -> Optional[str]:
        """和暦を西暦（YYYY-MM-DD）に変換"""
        if not wareki_str or wareki_str == "不明":
            return None
        
        # 令和
        match = re.match(r'令和(\d+)年(\d+)月(\d+)日', wareki_str)
        if match:
            reiwa_year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            year = 2018 + reiwa_year  # 令和元年 = 2019年
            return f"{year:04d}-{month:02d}-{day:02d}"
        
        # 平成
        match = re.match(r'平成(\d+)年(\d+)月(\d+)日', wareki_str)
        if match:
            heisei_year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            year = 1988 + heisei_year  # 平成元年 = 1989年
            return f"{year:04d}-{month:02d}-{day:02d}"
        
        return None
    
    def prepare_announcement_data(self, parsed_data: Dict) -> Dict:
        """告示データをBigQuery形式に変換"""
        announcement = parsed_data['announcement']
        
        return {
            'announcement_id': self._generate_announcement_id(announcement),
            'kanpo_date': announcement.get('kanpo_date'),
            'announcement_number': str(announcement.get('announcement_number', '不明')),
            'gazette_issue_number': str(announcement.get('gazette_issue_number', '')),
            'announcement_type': announcement.get('announcement_type', '発行告示'),
            'title': announcement.get('title', ''),
            'source_file': parsed_data['source_file'],
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }
    
    def prepare_issuance_data(self, announcement_id: str, issuances: List) -> List[Dict]:
        """銘柄データをBigQuery形式に変換"""
        result = []
        
        for idx, issuance in enumerate(issuances):
            issuance_id = f"{announcement_id}_ISSUE_{idx+1:03d}"
            
            # 和暦から西暦に変換
            issuance_date_str = self._convert_wareki_to_date(
                getattr(issuance, 'issuance_date', None)
            )
            maturity_date_str = self._convert_wareki_to_date(
                getattr(issuance, 'maturity_date', None)
            )
            payment_date_str = self._convert_wareki_to_date(
                getattr(issuance, 'payment_date', None)
            )
            
            data = {
                'issuance_id': issuance_id,
                'announcement_id': announcement_id,
                'bond_master_id': self._get_bond_master_id(issuance),
                'issuance_date': issuance_date_str,
                'maturity_date': maturity_date_str,
                'interest_rate': getattr(issuance, 'interest_rate', None),
                'issue_price': getattr(issuance, 'issue_price', None),
                'issue_amount': getattr(issuance, 'issue_amount', None),
                'payment_date': payment_date_str,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
            result.append(data)
        
        return result
    
    def prepare_legal_basis_data(self, issuance_data: List[Dict], issuances: List) -> List[Dict]:
        """法令根拠データをBigQuery形式に変換"""
        result = []
        
        for issuance_dict, issuance_obj in zip(issuance_data, issuances):
            legal_basis = getattr(issuance_obj, 'legal_basis', None)
            if not legal_basis:
                continue
            
            law_id, article_id = self._parse_legal_basis(legal_basis)
            
            if law_id and article_id:
                result.append({
                    'issuance_id': issuance_dict['issuance_id'],
                    'law_id': law_id,
                    'article_id': article_id,
                    'created_at': datetime.utcnow()
                })
        
        return result
    
    def insert_to_bigquery(self, table_name: str, data: List[Dict]) -> int:
        """BigQueryにデータを投入（強化されたリトライロジック）"""
        if not data:
            return 0
        
        table_id = f"{self.client.project}.{self.dataset_id}.{table_name}"
        df = pd.DataFrame(data)
        
        if table_name == 'announcements':
            df['kanpo_date'] = pd.to_datetime(df['kanpo_date'], errors='coerce').dt.date
            df['announcement_number'] = df['announcement_number'].astype(str)
            df['gazette_issue_number'] = df['gazette_issue_number'].fillna('').astype(str)
            df['title'] = df['title'].fillna('').astype(str)
        
        elif table_name == 'bond_issuances':
            # 既に西暦形式（YYYY-MM-DD）の文字列なので、そのまま変換
            df['issuance_date'] = pd.to_datetime(df['issuance_date'], errors='coerce').dt.date
            df['maturity_date'] = pd.to_datetime(df['maturity_date'], errors='coerce').dt.date
            df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce').dt.date
            df['interest_rate'] = pd.to_numeric(df['interest_rate'], errors='coerce')
            df['issue_price'] = pd.to_numeric(df['issue_price'], errors='coerce')
            df['issue_amount'] = pd.to_numeric(df['issue_amount'], errors='coerce')
        
        print(f"  📊 {table_name}: {len(df)} 件を投入中...")
        
        # 強化されたリトライロジック（5回、最大48秒待機）
        max_retries = 5
        base_wait_time = 3
        
        for attempt in range(max_retries):
            try:
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
                )
                
                job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
                job.result()
                
                print(f"  ✅ {table_name}: 投入成功")
                return len(data)
                
            except Exception as e:
                error_msg = str(e)
                
                if '503' in error_msg or 'Service Unavailable' in error_msg or 'timeout' in error_msg.lower():
                    if attempt < max_retries - 1:
                        wait_time = base_wait_time * (2 ** attempt)
                        print(f"  ⚠️ 一時的なエラー (試行 {attempt+1}/{max_retries})")
                        print(f"  ⏳ {wait_time}秒待機してリトライします...")
                        import time
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"  ❌ {table_name}: 最大リトライ回数に達しました")
                        raise
                else:
                    print(f"  ❌ {table_name}: 投入失敗 - {error_msg[:200]}")
                    raise
    
    def process_files(self, files: List[Path]):
        """ファイルを一括処理"""
        print("\n" + "="*60)
        print("📦 発行データ投入開始")
        print("="*60)
        
        announcements_buffer = []
        issuances_buffer = []
        legal_basis_buffer = []
        
        BATCH_SIZE = 10
        
        for idx, file_path in enumerate(files, 1):
            print(f"\n--- ファイル {idx}/{len(files)} ---")
            
            try:
                parsed_data = self.parse_kanpo_file(file_path)
                if not parsed_data:
                    self.stats['files_failed'] += 1
                    continue
                
                announcement = self.prepare_announcement_data(parsed_data)
                announcement_id = announcement['announcement_id']
                
                print(f"  告示ID: {announcement_id}")
                
                issuances = self.prepare_issuance_data(announcement_id, parsed_data['issuances'])
                legal_basis = self.prepare_legal_basis_data(issuances, parsed_data['issuances'])
                
                announcements_buffer.append(announcement)
                issuances_buffer.extend(issuances)
                legal_basis_buffer.extend(legal_basis)
                
                self.stats['files_processed'] += 1
                
                if len(announcements_buffer) >= BATCH_SIZE:
                    print(f"\n🔄 バッチ投入実行 ({len(announcements_buffer)} 告示)")
                    self._flush_buffers(announcements_buffer, issuances_buffer, legal_basis_buffer)
                    announcements_buffer = []
                    issuances_buffer = []
                    legal_basis_buffer = []
                
            except Exception as e:
                print(f"❌ ファイル処理エラー: {file_path.name}")
                print(f"   エラー詳細: {e}")
                self.stats['files_failed'] += 1
                self.stats['errors'].append({'file': file_path.name, 'error': str(e)})
        
        if announcements_buffer:
            print(f"\n🔄 最終バッチ投入 ({len(announcements_buffer)} 告示)")
            self._flush_buffers(announcements_buffer, issuances_buffer, legal_basis_buffer)
        
        self._print_summary()
    
    def _flush_buffers(self, announcements: List, issuances: List, legal_basis: List):
        """バッファのデータをBigQueryに投入"""
        if announcements:
            count = self.insert_to_bigquery('announcements', announcements)
            self.stats['announcements_inserted'] += count
        
        if issuances:
            count = self.insert_to_bigquery('bond_issuances', issuances)
            self.stats['issuances_inserted'] += count
        
        if legal_basis:
            count = self.insert_to_bigquery('issuance_legal_basis', legal_basis)
            self.stats['legal_basis_inserted'] += count
    
    def _print_summary(self):
        """処理結果のサマリーを表示"""
        print("\n" + "="*60)
        print("📊 投入結果サマリー")
        print("="*60)
        print(f"✅ 処理成功: {self.stats['files_processed']} ファイル")
        print(f"❌ 処理失敗: {self.stats['files_failed']} ファイル")
        print(f"📝 告示投入: {self.stats['announcements_inserted']} 件")
        print(f"💰 銘柄投入: {self.stats['issuances_inserted']} 件")
        print(f"📋 法令根拠投入: {self.stats['legal_basis_inserted']} 件")
        
        if self.stats['errors']:
            print(f"\n⚠️ エラー詳細 ({len(self.stats['errors'])}件):")
            for error in self.stats['errors'][:5]:
                print(f"  - {error['file']}: {error['error']}")
            if len(self.stats['errors']) > 5:
                print(f"  ... 他 {len(self.stats['errors'])-5} 件")
    
    def _generate_announcement_id(self, announcement: Dict) -> str:
        """告示IDを生成"""
        kanpo_date = announcement.get('kanpo_date', '')
        kanpo_date = str(kanpo_date).replace('-', '') if kanpo_date else '00000000'
        
        announcement_num = announcement.get('announcement_number', '000')
        if '第' in str(announcement_num) and '号' in str(announcement_num):
            announcement_num = str(announcement_num).replace('第', '').replace('号', '').strip()
        
        return f"ANN_{kanpo_date}_{announcement_num}"
    
    def _get_bond_master_id(self, issuance) -> Optional[str]:
        """銘柄マスタIDを取得"""
        bond_type = getattr(issuance, 'bond_type', '')
        type_mapping = {
            '利付国債': 'BOND_001',
            '物価連動国債': 'BOND_003',
            'GX債券': 'BOND_013',
            '国庫短期証券': 'BOND_014'
        }
        return type_mapping.get(bond_type, 'BOND_001')
    
    def _parse_legal_basis(self, legal_basis: str) -> tuple:
        """法令根拠を解析（複数法令の連結に対応）"""
        if not legal_basis or legal_basis == "不明":
            return (None, None)
        
        # 「及び」「並びに」で複数の法令が連結されている場合は分割
        # 「並びに」は大グループ、「及び」は小グループの区切りだが、
        # Day 3では簡易版として両方を同等に扱う
        legal_parts = re.split(r'(?:及び|並びに)', legal_basis)
        
        # 各パートを試行（特別会計に関する法律を優先）
        for part in legal_parts:
            part = part.strip()
            
            # 条項番号を抽出（第XX条、第XX条のXX、第XX条第X項）
            article_match = re.search(r'第(\d+)条(?:の(\d+))?', part)
            if not article_match:
                continue
            
            article_num = article_match.group(1)
            subsection = article_match.group(2)
            
            # 1. 財政法 + 第4条 → LAW_ZAISEI + ART_ZAISEI_4_1
            if '財政法' in part and article_num == '4':
                return ('LAW_ZAISEI', 'ART_ZAISEI_4_1')
            
            # 2-4. 特別会計に関する法律 + 第46条/第47条/第62条
            if '特別会計に関する法律' in part:
                if article_num == '46':
                    return ('LAW_TOKUBETSU', 'ART_TOKUBETSU_46_1')
                elif article_num == '47':
                    return ('LAW_TOKUBETSU', 'ART_TOKUBETSU_47_1')
                elif article_num == '62':
                    return ('LAW_TOKUBETSU', 'ART_TOKUBETSU_62_1')
            
            # 5. 財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律 + 第2条/第3条
            if '財政運営に必要な財源の確保を図るための公債の発行の特例に関する法律' in part:
                if article_num == '2':
                    return ('LAW_TOKUREIKOUSAI_H24', 'ART_TOKUREIKOUSAI_H24_2_1')
                elif article_num == '3':
                    # 第3条の場合、新しいarticle_idが必要だが、マスタに存在しないため
                    # 第2条として扱う（または None を返す）
                    return ('LAW_TOKUREIKOUSAI_H24', 'ART_TOKUREIKOUSAI_H24_2_1')
            
            # 6. 脱炭素成長型経済構造への円滑な移行の推進に関する法律 + 第7条
            if '脱炭素成長型経済構造への円滑な移行の推進に関する法律' in part and article_num == '7':
                return ('LAW_GX', 'ART_GX_7_1')
            
            # 7. 情報処理の促進に関する法律 + 第69条
            if '情報処理の促進に関する法律' in part and article_num == '69':
                return ('LAW_JOHO', 'ART_JOHO_69_1')
            
            # 8. 子ども・子育て支援法 + 第71条の26
            if '子ども' in part and '子育て支援法' in part and article_num == '71' and subsection == '26':
                return ('LAW_KODOMO', 'ART_KODOMO_71_26_1')
        
        return (None, None)


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(description='発行データをBigQueryに投入')
    parser.add_argument('--limit', type=int, default=None, help='処理するファイル数の上限')
    parser.add_argument('--data-dir', type=str, default=DATA_DIR, help='データディレクトリのパス')
    
    args = parser.parse_args()
    
    print("="*60)
    print("🚀 発行データBigQuery投入スクリプト")
    print("="*60)
    print(f"プロジェクト: {PROJECT_ID}")
    print(f"データセット: {DATASET_ID}")
    print(f"データディレクトリ: {args.data_dir}")
    if args.limit:
        print(f"⚠️ 制限モード: 最初の {args.limit} ファイルのみ処理")
    print()
    
    try:
        loader = IssuanceDataLoader(PROJECT_ID, DATASET_ID, SERVICE_ACCOUNT_KEY)
        files = loader.get_kanpo_files(args.data_dir, args.limit)
        
        if not files:
            print("❌ 処理対象のファイルがありません")
            return
        
        loader.process_files(files)
        print("\n✅ 処理が完了しました！")
        
    except Exception as e:
        print(f"\n❌ エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()