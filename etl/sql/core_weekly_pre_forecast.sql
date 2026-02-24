-- Weekly refresh before forecast
-- Includes: full core rebuild + weekly_consumption (codex_info baseline)

CREATE SCHEMA IF NOT EXISTS core;

CREATE OR REPLACE FUNCTION public.extract_color(material_name text) RETURNS text
LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    color TEXT;
BEGIN
    color := split_part(material_name, ' ', 1);
    color := split_part(color, '-', 1);
    color := trim(color);
    RETURN color;
END;
$$;

------------------------------------------------------------
-- 0) CORE şemasını ve RAW BOM kolonlarını garanti altına al
------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS core;

-- BOM'da renk kolonu yoksa ekle
ALTER TABLE core.bom_unique_materials
ADD COLUMN IF NOT EXISTS material_color TEXT;

-- Renk kolonunu fonksiyonla doldur (sadece NULL olanları)
UPDATE core.bom_unique_materials
SET material_color = extract_color(material_name)
WHERE material_color IS NULL;


------------------------------------------------------------
-- 1) ESKİ TABLOLARI TEMİZLE
--   (mapping + stok pipeline'ı baştan kuracağız)
------------------------------------------------------------

DROP TABLE IF EXISTS core.current_stock_by_variant CASCADE;
DROP TABLE IF EXISTS core.bom_stock_movements_normalized CASCADE;
DROP TABLE IF EXISTS core.bom_stock_movements_raw CASCADE;

DROP TABLE IF EXISTS core.bom_to_stock_map CASCADE;
DROP TABLE IF EXISTS core.match_nonfabric_variants CASCADE;
DROP TABLE IF EXISTS core.match_nonfabric_lvl1 CASCADE;
DROP TABLE IF EXISTS core.match_fabric_lvl3 CASCADE;
DROP TABLE IF EXISTS core.match_fabric_lvl2 CASCADE;
DROP TABLE IF EXISTS core.match_fabric_lvl1 CASCADE;




------------------------------------------------------------
-- 3) MATCH TABLOLARI (EK_2 EN BAŞTAN TEXT)
------------------------------------------------------------

CREATE TABLE core.match_fabric_lvl1 (
    bom_material_name   TEXT,
    bom_material_color  TEXT,
    bom_item_no         TEXT,
    bom_uom             TEXT,
    bom_type            TEXT,
    stock_adi           TEXT,
    ek_1                TEXT,
    ek_2                TEXT,
    turu3               TEXT,
    match_level         INT
);

CREATE TABLE core.match_fabric_lvl2       (LIKE core.match_fabric_lvl1);
CREATE TABLE core.match_fabric_lvl3       (LIKE core.match_fabric_lvl1);
CREATE TABLE core.match_nonfabric_lvl1    (LIKE core.match_fabric_lvl1);
CREATE TABLE core.match_nonfabric_variants(LIKE core.match_fabric_lvl1);

CREATE TABLE core.bom_to_stock_map (
    bom_material_name   TEXT,
    bom_material_color  TEXT,
    bom_item_no         TEXT,
    bom_uom             TEXT,
    bom_type            TEXT,
    stock_adi           TEXT,
    ek_1                TEXT,
    ek_2                TEXT,
    turu3               TEXT,
    match_level         INT
);


------------------------------------------------------------
-- 4) MATCH TABLOLARINI POPULATE (DUPLICATE KONTROLLÜ)
------------------------------------------------------------
-- KUMAŞ LEVEL 1: ADI = material_name, item_no dolu olanlar
------------------------------------------------------------

INSERT INTO core.match_fabric_lvl1
SELECT
    b.material_name,
    b.material_color,
    b.item_no,
    b.unit_of_measure,
    'KUMAŞ' AS bom_type,
    s.adi,
    s.ek_1,
    s.ek_2::text,
    s.turu3,
    1 AS match_level
FROM core.bom_unique_materials b
JOIN raw.stock_master s
      ON s.adi = b.material_name
WHERE b.material_category LIKE 'KUMA%'
  AND b.material_name IS NOT NULL
  AND b.material_name <> ''
  AND b.item_no <> '';


------------------------------------------------------------
-- KUMAŞ LEVEL 2: renk + item_no eşleşmesi
--  - Sadece ADI eşleşmesi bulunmayan KUMAŞ'lar
--  - Aynı bom_material_name + stock_adi zaten L1'de varsa eklenmez
------------------------------------------------------------

INSERT INTO core.match_fabric_lvl2
SELECT
    b.material_name,
    b.material_color,
    b.item_no,
    b.unit_of_measure,
    'KUMAŞ' AS bom_type,
    s.adi,
    s.ek_1,
    s.ek_2::text,
    s.turu3,
    2 AS match_level
FROM core.bom_unique_materials b
JOIN raw.stock_master s
        ON s.ek_1 = b.material_color
       AND s.turu3 = b.item_no
WHERE b.material_category LIKE 'KUMA%'
  AND b.material_name IS NOT NULL
  AND b.material_name <> ''
  AND b.item_no <> ''
  AND NOT EXISTS (
        SELECT 1
        FROM raw.stock_master s2
        WHERE s2.adi = b.material_name
  )
  AND NOT EXISTS (
        SELECT 1
        FROM core.match_fabric_lvl1 m
        WHERE m.bom_material_name = b.material_name
          AND m.stock_adi = s.adi
  );


------------------------------------------------------------
-- KUMAŞ LEVEL 3: item_no boş, ek_1 = material_name
--  - L1 ve L2'de aynı stok_adi zaten eklenmişse tekrar eklenmez
------------------------------------------------------------

INSERT INTO core.match_fabric_lvl3
SELECT
    b.material_name,
    b.material_color,
    b.item_no,
    b.unit_of_measure,
    'KUMAŞ' AS bom_type,
    s.adi,
    s.ek_1,
    s.ek_2::text,
    s.turu3,
    3 AS match_level
FROM core.bom_unique_materials b
JOIN raw.stock_master s
      ON s.ek_1 = b.material_name
WHERE b.material_category LIKE 'KUMA%'
  AND b.material_name IS NOT NULL
  AND b.material_name <> ''
  AND (b.item_no IS NULL OR b.item_no = '')
  AND NOT EXISTS (
        SELECT 1
        FROM (
            SELECT bom_material_name, stock_adi
            FROM core.match_fabric_lvl1
            UNION ALL
            SELECT bom_material_name, stock_adi
            FROM core.match_fabric_lvl2
        ) x
        WHERE x.bom_material_name = b.material_name
          AND x.stock_adi        = s.adi
  );


------------------------------------------------------------
-- NON-FABRIC LEVEL 1: ADI = material_name
------------------------------------------------------------

INSERT INTO core.match_nonfabric_lvl1
SELECT
    b.material_name,
    b.material_color,
    b.item_no,
    b.unit_of_measure,
    b.material_category AS bom_type,
    s.adi,
    s.ek_1,
    s.ek_2::text,
    s.turu3,
    1 AS match_level
FROM core.bom_unique_materials b
JOIN raw.stock_master s
      ON s.adi = b.material_name
WHERE b.material_name IS NOT NULL
  AND b.material_name <> ''
  AND b.material_category NOT LIKE 'KUMA%';


------------------------------------------------------------
-- NON-FABRIC VARIANTS:
--  - ek_1 & ek_2 & turu3 aynı olan diğer stok kartları
--  - Aynı bom_material_name + stock_adi daha önce L1'de eklenmişse eklenmez
------------------------------------------------------------

INSERT INTO core.match_nonfabric_variants
SELECT
    p.bom_material_name,
    p.bom_material_color,
    p.bom_item_no,
    p.bom_uom,
    p.bom_type,
    s2.adi AS stock_adi,
    s2.ek_1,
    s2.ek_2::text,
    s2.turu3,
    2 AS match_level
FROM core.match_nonfabric_lvl1 p
JOIN raw.stock_master s1
      ON s1.adi = p.stock_adi
JOIN raw.stock_master s2
      ON s2.ek_1 = s1.ek_1
     AND s2.ek_2 = s1.ek_2
     AND s2.turu3 = s1.turu3
     AND s1.turu3 <> ''  -- turu3 boş olanları dahil etme
WHERE NOT EXISTS (
        SELECT 1
        FROM core.match_nonfabric_lvl1 m
        WHERE m.bom_material_name = p.bom_material_name
          AND m.stock_adi         = s2.adi
);


------------------------------------------------------------
-- 5) ANA MAPPING TABLOSU
------------------------------------------------------------

TRUNCATE core.bom_to_stock_map;

INSERT INTO core.bom_to_stock_map
SELECT * FROM core.match_fabric_lvl1
UNION ALL
SELECT * FROM core.match_fabric_lvl2
UNION ALL
SELECT * FROM core.match_fabric_lvl3
UNION ALL
SELECT * FROM core.match_nonfabric_lvl1
UNION ALL
SELECT * FROM core.match_nonfabric_variants;


------------------------------------------------------------
-- 6) INDEXLER
------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_bum_name        ON core.bom_unique_materials(material_name);
CREATE INDEX IF NOT EXISTS idx_map_bom         ON core.bom_to_stock_map(bom_material_name);
CREATE INDEX IF NOT EXISTS idx_map_stock       ON core.bom_to_stock_map(stock_adi);

CREATE INDEX IF NOT EXISTS idx_sm_adi          ON raw.stock_master(adi);
CREATE INDEX IF NOT EXISTS idx_sm_ek1_turu3    ON raw.stock_master(ek_1, turu3);
CREATE INDEX IF NOT EXISTS idx_sm_ek1_ek2_turu3 ON raw.stock_master(ek_1, ek_2, turu3);






CREATE TABLE core.current_stock_by_variant (
    bom_material_name TEXT,
    bom_uom           TEXT,
    bom_type          TEXT,
    stock_adi         TEXT,
    stock_uom         TEXT,
    warehouse         TEXT,
    current_stock     NUMERIC
);

INSERT INTO core.current_stock_by_variant
SELECT
    b.bom_material_name,
    b.bom_uom,
    b.bom_type,
    r.stock_adi,
    r.stock_uom,
    r.warehouse,
    CASE
        WHEN b.bom_type LIKE 'KUMAŞ'
         AND b.bom_uom  = 'Mt2'
         AND (r.stock_uom ILIKE '%mt%' AND r.stock_uom NOT ILIKE '%mt2%')
        THEN
            r.current_stock
            * (
                NULLIF(
                    REGEXP_REPLACE(b.ek_2, '[^0-9\.]', '', 'g'),
                    ''
                )::NUMERIC
              / 100.0
              )
        ELSE
            r.current_stock
    END AS current_stock
FROM core.bom_to_stock_map b
JOIN core.raw_current_stock r
      ON b.stock_adi = r.stock_adi;


DROP TABLE IF EXISTS core.weekly_consumption;

CREATE TABLE core.weekly_consumption (
    bom_material_name       text,
    bom_material_category   text,
    bom_item_no             text,
    bom_unit_of_measure     text,
    week_start              date,
    qty                     numeric
);


INSERT INTO core.weekly_consumption (
    bom_material_name,
    bom_material_category,
    bom_item_no,
    bom_unit_of_measure,
    week_start,
    qty
)
WITH 
max_dates AS (
    SELECT
        MAX(transaction_date)::date AS max_tx_date,
        date_trunc('week', MAX(transaction_date))::date AS max_week_start
    FROM raw.raw_bom_consumption
),
cutoff AS (
    SELECT
        CASE
            WHEN max_tx_date >= (max_week_start + INTERVAL '4 days') THEN max_week_start
            ELSE (max_week_start - INTERVAL '7 days')
        END AS last_full_week_start
    FROM max_dates
),
-- 1) Her malzeme için kategori + item_no + uom mapping’i
material_map AS (
    SELECT 
        material_name AS bom_material_name,
        MIN(material_category) AS bom_material_category,
        MIN(item_no) AS bom_item_no,
        MIN(unit_of_measure) AS bom_unit_of_measure
    FROM raw.raw_bom_consumption
    GROUP BY material_name
),

-- 2) Gerçek haftalık tüketim
base AS (
    SELECT
        material_name AS bom_material_name,
        date_trunc('week', transaction_date)::date AS week_start,
        SUM(quantity) AS qty
    FROM raw.raw_bom_consumption, cutoff
    WHERE date_trunc('week', transaction_date)::date <= cutoff.last_full_week_start
    GROUP BY material_name, date_trunc('week', transaction_date)::date
),

-- 3) Tüm haftalar
all_weeks AS (
    SELECT DISTINCT date_trunc('week', transaction_date)::date AS week_start
    FROM raw.raw_bom_consumption, cutoff
    WHERE date_trunc('week', transaction_date)::date <= cutoff.last_full_week_start
),

-- 4) Tüm materyaller (mapping ile)
all_materials AS (
    SELECT 
        mm.bom_material_name,
        mm.bom_material_category,
        mm.bom_item_no,
        mm.bom_unit_of_measure
    FROM material_map mm
),

-- 5) Full matrix (her malzeme × her hafta)
full_matrix AS (
    SELECT 
        a.bom_material_name,
        a.bom_material_category,
        a.bom_item_no,
        a.bom_unit_of_measure,
        w.week_start
    FROM all_materials a CROSS JOIN all_weeks w
),

-- 6) Full matrix + gerçek değer join
joined AS (
    SELECT 
        f.bom_material_name,
        f.bom_material_category,
        f.bom_item_no,
        f.bom_unit_of_measure,
        f.week_start,
        COALESCE(b.qty, 0) AS qty
    FROM full_matrix f
    LEFT JOIN base b 
        ON b.bom_material_name = f.bom_material_name
       AND b.week_start       = f.week_start
)

SELECT * FROM joined;

CREATE INDEX idx_weekly_material_week
ON core.weekly_consumption (bom_material_name, week_start);

CREATE INDEX idx_weekly_category
ON core.weekly_consumption (bom_material_category);

CREATE INDEX idx_weekly_itemno
ON core.weekly_consumption (bom_item_no);
