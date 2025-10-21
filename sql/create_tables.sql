-- ==========================================
-- JGB Database Project - Table Creation DDL
-- Phase 1: 2023年度 国債発行データベース
-- ==========================================

-- ===================
-- 1. 法令マスタ
-- ===================
CREATE TABLE IF NOT EXISTS `jgb2023.20251019.laws_master` (
  law_id STRING NOT NULL OPTIONS(description="法令ID（主キー）例: LAW_ZAISEI, LAW_TOKUBETSU, LAW_TOKUREIKOUSAI_R5"),
  law_name STRING NOT NULL OPTIONS(description="法令名 例: 財政法、特別会計に関する法律、令和五年度特例公債法"),
  law_type STRING OPTIONS(description="法令種別 例: 法律、政令、省令"),
  law_number STRING OPTIONS(description="法令番号 例: 昭和22年法律第34号"),
  promulgation_date DATE OPTIONS(description="公布年月日"),
  enforcement_date DATE OPTIONS(description="施行年月日"),
  description STRING OPTIONS(description="法令概要・目的"),
  effective_from DATE OPTIONS(description="有効期間開始日（特例公債法等の年度法令用）"),
  effective_to DATE OPTIONS(description="有効期間終了日（特例公債法等の年度法令用）"),
  is_active BOOL DEFAULT TRUE OPTIONS(description="有効フラグ"),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) 
PARTITION BY DATE(created_at)
OPTIONS(
  description="法令マスタ - 国債発行の根拠となる法令情報",
  labels=[("phase", "1"), ("category", "master")]
);

-- ===================
-- 2. 法令条項マスタ
-- ===================
CREATE TABLE IF NOT EXISTS `jgb2023.20251019.law_articles_master` (
  article_id STRING NOT NULL OPTIONS(description="条項ID（主キー）例: LAW_ZAISEI_ART4"),
  law_id STRING NOT NULL OPTIONS(description="法令ID（外部キー）"),
  article_number STRING OPTIONS(description="条文番号 例: 第4条、第5条"),
  paragraph_number STRING OPTIONS(description="項番号 例: 第1項、第2項"),
  item_number STRING OPTIONS(description="号番号 例: 第1号、第2号"),
  article_text STRING OPTIONS(description="条文テキスト"),
  purpose STRING OPTIONS(description="発行目的・用途 例: 公共事業費、出資金、貸付金"),
  bond_type STRING OPTIONS(description="対応する国債種別 例: 建設国債、赤字国債"),
  effective_from DATE OPTIONS(description="有効期間開始日"),
  effective_to DATE OPTIONS(description="有効期間終了日"),
  is_active BOOL DEFAULT TRUE OPTIONS(description="有効フラグ"),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
OPTIONS(
  description="法令条項マスタ - 法令内の具体的な条項情報と発行根拠",
  labels=[("phase", "1"), ("category", "master")]
);

-- ===================
-- 3. 銘柄マスタ
-- ===================
CREATE TABLE IF NOT EXISTS `jgb2023.20251019.bonds_master` (
  bond_id STRING NOT NULL OPTIONS(description="銘柄ID（主キー）例: BOND_KENSETSU_10Y, BOND_AKAJI_2Y"),
  bond_name STRING NOT NULL OPTIONS(description="銘柄名 例: 10年利付国債（建設）、2年利付国債（赤字）"),
  bond_type STRING OPTIONS(description="銘柄区分 例: 建設国債、赤字国債、物価連動国債"),
  maturity_years INT64 OPTIONS(description="償還年限 例: 2, 5, 10, 20, 30, 40"),
  maturity_type STRING OPTIONS(description="償還期限区分 例: 2年、5年、10年、20年、30年、40年"),
  issue_method STRING OPTIONS(description="発行方式 例: 入札、引受、公募"),
  interest_type STRING OPTIONS(description="利付区分 例: 利付、割引"),
  interest_payment STRING OPTIONS(description="利払方法 例: 年2回、満期一括"),
  min_denomination NUMERIC OPTIONS(description="最低額面金額"),
  description STRING OPTIONS(description="銘柄説明"),
  is_active BOOL DEFAULT TRUE OPTIONS(description="有効フラグ"),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
OPTIONS(
  description="銘柄マスタ - 国債の銘柄分類情報",
  labels=[("phase", "1"), ("category", "master")]
);

-- ===================
-- 4. 告示テーブル
-- ===================
CREATE TABLE IF NOT EXISTS `jgb2023.20251019.announcements` (
  announcement_id STRING NOT NULL OPTIONS(description="告示ID（主キー）例: ANN_2023_MOF_123"),
  kanpo_date DATE NOT NULL OPTIONS(description="官報発行日"),
  kanpo_number STRING OPTIONS(description="官報番号 例: 号外第123号"),
  announcement_number STRING OPTIONS(description="告示番号 例: 財務省告示第123号"),
  ministry STRING OPTIONS(description="発行省庁 例: 財務省"),
  ministry_code STRING OPTIONS(description="省庁コード 例: MOF"),
  announcement_type STRING OPTIONS(description="告示種別 例: 国債発行、償還"),
  title STRING OPTIONS(description="告示タイトル"),
  content STRING OPTIONS(description="告示本文（全文）"),
  fiscal_year INT64 OPTIONS(description="対象会計年度 例: 2023"),
  source_file STRING OPTIONS(description="元データファイル名"),
  parsed_at TIMESTAMP OPTIONS(description="パース実行日時"),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY kanpo_date
CLUSTER BY fiscal_year, ministry_code
OPTIONS(
  description="告示テーブル - 官報に掲載された国債関連告示情報",
  labels=[("phase", "1"), ("category", "data")]
);

-- ===================
-- 5. 発行銘柄テーブル
-- ===================
CREATE TABLE IF NOT EXISTS `jgb2023.20251019.bond_issuances` (
  issuance_id STRING NOT NULL OPTIONS(description="発行ID（主キー）例: ISS_2023_001"),
  announcement_id STRING NOT NULL OPTIONS(description="告示ID（外部キー）"),
  bond_id STRING OPTIONS(description="銘柄ID（外部キー）"),
  series_number STRING OPTIONS(description="シリーズ番号 例: 第123回"),
  issue_date DATE OPTIONS(description="発行日"),
  maturity_date DATE OPTIONS(description="償還日"),
  issue_amount NUMERIC(20,2) OPTIONS(description="発行額（円）"),
  interest_rate NUMERIC(10,6) OPTIONS(description="表面利率（%）"),
  issue_price NUMERIC(10,4) OPTIONS(description="発行価格（額面100円あたり）"),
  average_price NUMERIC(10,4) OPTIONS(description="平均落札価格（入札の場合）"),
  lowest_price NUMERIC(10,4) OPTIONS(description="最低落札価格（入札の場合）"),
  competitive_amount NUMERIC(20,2) OPTIONS(description="競争入札額"),
  noncompetitive_amount NUMERIC(20,2) OPTIONS(description="非競争入札額"),
  denomination NUMERIC(20,2) OPTIONS(description="額面金額単位"),
  fiscal_year INT64 OPTIONS(description="会計年度"),
  remarks STRING OPTIONS(description="備考"),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY issue_date
CLUSTER BY fiscal_year, bond_id
OPTIONS(
  description="発行銘柄テーブル - 個別の国債発行案件情報",
  labels=[("phase", "1"), ("category", "data")]
);

-- ===================
-- 6. 発行根拠テーブル
-- ===================
CREATE TABLE IF NOT EXISTS `jgb2023.20251019.issuance_legal_basis` (
  basis_id STRING NOT NULL OPTIONS(description="根拠ID（主キー）例: BASIS_2023_001"),
  issuance_id STRING NOT NULL OPTIONS(description="発行ID（外部キー）"),
  law_id STRING NOT NULL OPTIONS(description="法令ID（外部キー）"),
  article_id STRING OPTIONS(description="条項ID（外部キー）"),
  allocation_amount NUMERIC(20,2) OPTIONS(description="当該法令による発行額（円）"),
  allocation_ratio NUMERIC(5,4) OPTIONS(description="配分比率（0-1）"),
  remarks STRING OPTIONS(description="備考"),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
CLUSTER BY issuance_id, law_id
OPTIONS(
  description="発行根拠テーブル - 発行案件と法的根拠の紐付け（銘柄×法令の交差テーブル）",
  labels=[("phase", "1"), ("category", "data")]
);

-- ===================
-- インデックス作成
-- ===================

-- announcements テーブル
CREATE INDEX IF NOT EXISTS idx_announcements_fiscal_year
ON `jgb2023.20251019.announcements`(fiscal_year);

CREATE INDEX IF NOT EXISTS idx_announcements_announcement_number
ON `jgb2023.20251019.announcements`(announcement_number);

-- bond_issuances テーブル
CREATE INDEX IF NOT EXISTS idx_bond_issuances_announcement_id
ON `jgb2023.20251019.bond_issuances`(announcement_id);

CREATE INDEX IF NOT EXISTS idx_bond_issuances_bond_id
ON `jgb2023.20251019.bond_issuances`(bond_id);

-- issuance_legal_basis テーブル
CREATE INDEX IF NOT EXISTS idx_legal_basis_issuance_id
ON `jgb2023.20251019.issuance_legal_basis`(issuance_id);

CREATE INDEX IF NOT EXISTS idx_legal_basis_law_id
ON `jgb2023.20251019.issuance_legal_basis`(law_id);

-- ===================
-- テーブル作成完了確認
-- ===================
SELECT 
  table_name,
  table_type,
  creation_time,
  row_count
FROM `jgb2023.20251019.__TABLES__`
ORDER BY creation_time DESC;