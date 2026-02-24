-- Remove cancelled stock movements and rebuild dependent stock aggregates.
-- Adjust schema names if needed.

BEGIN;

-- 1) Delete cancelled movements from raw stock movements.
DELETE FROM raw.raw_stock_movements
WHERE movement_status NOT IN ('Aktif', 'Sipar', 'Son')
   OR movement_status IS NULL;

-- 2) Rebuild raw_current_stock from remaining movements.
TRUNCATE TABLE core.raw_current_stock;

INSERT INTO core.raw_current_stock (stock_adi, warehouse, stock_uom, current_stock)
SELECT
    material_name AS stock_adi,
    company_code  AS warehouse,
    unit_of_measure AS stock_uom,
    SUM(
        CASE
            WHEN document_type = 'Depo Çıkış' THEN -quantity
            ELSE quantity
        END
    ) AS current_stock
FROM raw.raw_stock_movements
WHERE company_code NOT IN (SELECT warehouse FROM core.seat_warehouses)
GROUP BY material_name, company_code, unit_of_measure;

-- 3) Rebuild current_stock_by_variant from the refreshed raw_current_stock.
TRUNCATE TABLE core.current_stock_by_variant;

INSERT INTO core.current_stock_by_variant
SELECT
    b.bom_material_name,
    b.bom_uom,
    b.bom_type,
    r.stock_adi,
    r.stock_uom,
    r.warehouse,
    CASE
        WHEN b.bom_type LIKE 'KUMA%'
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

-- 4) Sync incremental state to avoid reprocessing old H_IDs.
INSERT INTO core.sync_hid_state (name, last_hid, updated_at)
VALUES ('raw_current_stock', (SELECT COALESCE(MAX(h_id), 0) FROM raw.raw_stock_movements), NOW())
ON CONFLICT (name) DO UPDATE
SET last_hid = EXCLUDED.last_hid,
    updated_at = NOW();

INSERT INTO core.sync_hid_state (name, last_hid, updated_at)
VALUES ('current_stock_by_variant', (SELECT COALESCE(MAX(h_id), 0) FROM raw.raw_stock_movements), NOW())
ON CONFLICT (name) DO UPDATE
SET last_hid = EXCLUDED.last_hid,
    updated_at = NOW();

COMMIT;
