"""
Phase 4 統合パーサー: Universal Announcement Parser
4つのパーサーを統合し、自動判定機能を実装
"""

import re
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import os
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\sonke\secrets\jgb2023-f8c9b849ae2d.json'

PROJECT_ID = "jgb2023"
DATASET_ID = "20251027"


# 既存の4つのパーサーをインポート（簡略版をここに含める）
class NumberedListParser:
    """番号付きリスト形式（複数法令）"""
    
    def can_parse(self, text: str) -> float:
        """処理可能性スコア（0.0～1.0）"""
        score = 0.0
        
        # 価格競争入札 + 非価格競争入札
        if '価格競争入札' in text and '非価格競争入札' in text:
            score += 0.5
        
        # 複数法令（並びに、及び）
        if '並びに' in text and '及び' in text:
            score += 0.3
        
        # 別表なし
        if '別表' not in text:
            score += 0.2
        
        return min(score, 1.0)
    
    def extract(self, text: str) -> Dict[str, Any]:
        """データ抽出（簡略版）"""
        # 実際の実装は prototype_v4_parser.py を参照
        return {'pattern': 'NUMBERED_LIST_MULTI_LAW', 'items': {}}


class TableParserV4:
    """横並び別表形式"""
    
    def can_parse(self, text: str) -> float:
        """処理可能性スコア（0.0～1.0）"""
        score = 0.0
        
        # 別表の存在
        if '（別表のとおり）' in text or '内訳（別表のとおり）' in text:
            score += 0.4
        
        # 別表の実データ
        if '名称及び記号' in text and '利率' in text and '償還期限' in text:
            score += 0.6
        
        return min(score, 1.0)
    
    def extract(self, text: str) -> Dict[str, Any]:
        """データ抽出（簡略版）"""
        return {'pattern': 'TABLE_HORIZONTAL', 'items': {}}


class RetailBondParser:
    """個人向け国債"""
    
    def can_parse(self, text: str) -> float:
        """処理可能性スコア（0.0～1.0）"""
        if '個人向け利付国庫債券' in text or '個人向け国債' in text:
            return 1.0
        return 0.0
    
    def extract(self, text: str) -> Dict[str, Any]:
        """データ抽出（簡略版）"""
        return {'pattern': 'RETAIL_BOND', 'items': {}}


class TBParser:
    """国庫短期証券"""
    
    def can_parse(self, text: str) -> float:
        """処理可能性スコア（0.0～1.0）"""
        score = 0.0
        
        if '国庫短期証券' in text:
            score += 0.5
        
        if '割引短期国債' in text or '政府短期証券' in text:
            score += 0.5
        
        return min(score, 1.0)
    
    def extract(self, text: str) -> Dict[str, Any]:
        """データ抽出（簡略版）"""
        return {'pattern': 'TB_SHORT_TERM', 'items': {}}


class UniversalAnnouncementParser:
    """統合パーサー：すべてのパターンに対応"""
    
    def __init__(self):
        # 4つのパーサーを登録（優先順位順）
        self.parsers = [
            ('RETAIL_BOND', RetailBondParser()),
            ('TB_SHORT_TERM', TBParser()),
            ('TABLE_HORIZONTAL', TableParserV4()),
            ('NUMBERED_LIST_MULTI_LAW', NumberedListParser()),
        ]
    
    def identify_pattern(self, text: str) -> tuple[str, float]:
        """
        パターンを自動識別
        
        Returns:
            (pattern_name, confidence_score)
        """
        scores = {}
        
        for pattern_name, parser in self.parsers:
            score = parser.can_parse(text)
            scores[pattern_name] = score
        
        # 最高スコアのパターンを選択
        best_pattern = max(scores.items(), key=lambda x: x[1])
        
        return best_pattern[0], best_pattern[1]
    
    def parse(self, text: str) -> Dict[str, Any]:
        """
        告示を解析
        
        Returns:
            {
                'pattern': パターン名,
                'confidence': 信頼度,
                'items': 抽出データ,
                'error': エラーメッセージ（あれば）
            }
        """
        try:
            # パターン識別
            pattern, confidence = self.identify_pattern(text)
            
            # 信頼度が低すぎる場合は警告
            if confidence < 0.5:
                return {
                    'pattern': 'UNKNOWN',
                    'confidence': confidence,
                    'items': {},
                    'error': f'パターン識別の信頼度が低い（{confidence:.2f}）'
                }
            
            # 適切なパーサーを選択
            parser = None
            for p_name, p in self.parsers:
                if p_name == pattern:
                    parser = p
                    break
            
            if parser is None:
                return {
                    'pattern': pattern,
                    'confidence': confidence,
                    'items': {},
                    'error': f'パーサーが見つかりません: {pattern}'
                }
            
            # データ抽出
            items = parser.extract(text)
            
            return {
                'pattern': pattern,
                'confidence': confidence,
                'items': items.get('items', {}),
                'error': None
            }
        
        except Exception as e:
            return {
                'pattern': 'ERROR',
                'confidence': 0.0,
                'items': {},
                'error': str(e)
            }


def batch_process(
    input_dir: Path,
    dataset_id: str = DATASET_ID,
    test_mode: bool = True,
    max_files: int = 10
) -> Dict[str, Any]:
    """
    バッチ処理：複数ファイルを一括処理
    
    Args:
        input_dir: 入力ディレクトリ
        dataset_id: BigQueryデータセットID
        test_mode: テストモード（Trueで最大max_files件）
        max_files: テストモード時の最大処理件数
    
    Returns:
        処理結果の統計情報
    """
    parser = UniversalAnnouncementParser()
    client = bigquery.Client(project=PROJECT_ID)
    
    # ファイル一覧取得
    files = sorted(input_dir.glob('*.txt'))
    
    if test_mode:
        files = files[:max_files]
        print(f"🧪 テストモード: 最初の{len(files)}ファイルを処理")
    else:
        print(f"🚀 本番モード: {len(files)}ファイルを処理")
    
    print()
    
    # 統計情報
    stats = {
        'total': len(files),
        'success': 0,
        'failed': 0,
        'by_pattern': {},
        'errors': []
    }
    
    for i, file_path in enumerate(files, 1):
        print(f"[{i}/{len(files)}] {file_path.name}")
        
        try:
            # ファイル読み込み
            with open(file_path, 'r', encoding='utf-8') as f:
                text = f.read()
            
            # パース
            result = parser.parse(text)
            
            pattern = result['pattern']
            confidence = result['confidence']
            
            print(f"  パターン: {pattern} (信頼度: {confidence:.2f})")
            
            # 統計更新
            if pattern not in stats['by_pattern']:
                stats['by_pattern'][pattern] = 0
            stats['by_pattern'][pattern] += 1
            
            if result['error']:
                print(f"  ❌ エラー: {result['error']}")
                stats['failed'] += 1
                stats['errors'].append({
                    'file': file_path.name,
                    'pattern': pattern,
                    'error': result['error']
                })
            else:
                print(f"  ✅ 成功")
                stats['success'] += 1
            
            print()
        
        except Exception as e:
            print(f"  ❌ 例外: {e}")
            stats['failed'] += 1
            stats['errors'].append({
                'file': file_path.name,
                'pattern': 'EXCEPTION',
                'error': str(e)
            })
            print()
    
    # 結果サマリー
    print("=" * 70)
    print("📊 処理結果サマリー")
    print("=" * 70)
    print(f"総ファイル数: {stats['total']}")
    print(f"成功: {stats['success']} ({stats['success']/stats['total']*100:.1f}%)")
    print(f"失敗: {stats['failed']} ({stats['failed']/stats['total']*100:.1f}%)")
    print()
    
    print("パターン別:")
    for pattern, count in sorted(stats['by_pattern'].items(), key=lambda x: x[1], reverse=True):
        print(f"  {pattern}: {count} ({count/stats['total']*100:.1f}%)")
    print()
    
    if stats['errors']:
        print(f"エラー詳細（最初の5件）:")
        for error in stats['errors'][:5]:
            print(f"  - {error['file']}: {error['error'][:50]}...")
        print()
    
    return stats


def test_universal_parser():
    """統合パーサーのテスト"""
    
    print("=" * 70)
    print("Phase 4 統合パーサー テスト")
    print("=" * 70)
    print()
    
    # テストケース
    test_files = [
        Path(r"G:\マイドライブ\JGBデータ\2023\20230915_令和5年10月11日付（財務省第二百五十一号）.txt"),
        Path(r"G:\マイドライブ\JGBデータ\2023\20230414_令和5年5月9日付（財務省第百二十七号）.txt"),
        Path(r"G:\マイドライブ\JGBデータ\2023\20230615_令和5年7月11日付（財務省第百九十二号）.txt"),
        Path(r"G:\マイドライブ\JGBデータ\2023\20231211_令和6年1月12日付（財務省第十六号）.txt"),
    ]
    
    parser = UniversalAnnouncementParser()
    
    for file_path in test_files:
        if not file_path.exists():
            print(f"⚠️  ファイルが見つかりません: {file_path.name}")
            continue
        
        print(f"📄 {file_path.name}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()
        
        result = parser.parse(text)
        
        print(f"  パターン: {result['pattern']}")
        print(f"  信頼度: {result['confidence']:.2f}")
        
        if result['error']:
            print(f"  エラー: {result['error']}")
        else:
            print(f"  ✅ パース成功")
        
        print()
    
    print("=" * 70)
    print("✅ テスト完了")
    print("=" * 70)


if __name__ == "__main__":
    # 統合パーサーのテスト
    test_universal_parser()
    
    print()
    print("=" * 70)
    print("次のステップ:")
    print("  1. batch_process() でバッチ処理をテスト")
    print("  2. 実際のパーサー実装を統合")
    print("  3. 全件処理（179ファイル）")
    print("=" * 70)