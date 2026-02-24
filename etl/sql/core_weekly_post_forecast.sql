-- Weekly refresh after forecast
-- Depends on: core.final_forecast_summary being up to date

-- MATERIAL DASHBOARD VARIANTS: 2 Options, first is mainstream

DROP TABLE IF EXISTS core.dashboard_material_variants_new;

CREATE TABLE core.dashboard_material_variants_new AS
WITH allowed_warehouses AS (
    SELECT unnest(ARRAY[
        'WAREHOUSE22',
        'JALUZİ KOLTUK DEPO',
        'KATLAMALI KOLTUK DEPO',
        'STOR KOLTUK DEPO'
    ]) AS warehouse
),

variants AS (

    SELECT
        b.material_name AS bom_material_name,
        s.adi           AS stock_adi
    FROM core.bom_unique_materials b
    JOIN raw.stock_master s
      ON s.ek_1 = b.material_color
     AND (
            /* 1️⃣ KUMAŞ → item_no'ya bakma */
            b.material_category = 'KUMAŞ'

            /* 2️⃣ KUMAŞ DIŞI → item_no doluysa net eşleşme */
         OR (
                b.material_category <> 'KUMAŞ'
            AND b.item_no IS NOT NULL
            AND b.item_no <> ''
            AND s.turu3 = b.item_no
         )

            /* 3️⃣ KUMAŞ DIŞI → item_no boşsa renge göre */
         OR (
                b.material_category <> 'KUMAŞ'
            AND (b.item_no IS NULL OR b.item_no = '')
         )
        )
)

,open_orders AS (
    SELECT
        h_id,
        material_name AS stock_adi,
        unit_of_measure,
        CASE
            WHEN unit_of_measure = 'PktAdtMt' THEN 'Mt'
            WHEN unit_of_measure = 'PktAdt' THEN 'Adet'
            ELSE unit_of_measure
        END AS unit_norm,
        transaction_date,
        SUM(quantity) AS open_qty
    FROM raw.raw_open_order_movements
    GROUP BY h_id, material_name, unit_of_measure, unit_norm, transaction_date
),
open_orders_with_next AS (
    SELECT
        o.*,
        LEAD(o.transaction_date) OVER (
            PARTITION BY o.stock_adi, o.unit_norm
            ORDER BY o.transaction_date, o.h_id
        ) AS next_order_date
    FROM open_orders o
),
matched_receipts AS (
    SELECT
        r.ref_hid AS h_id,
        r.material_name AS stock_adi,
        CASE
            WHEN r.unit_of_measure = 'PktAdtMt' THEN 'Mt'
            WHEN r.unit_of_measure = 'PktAdt' THEN 'Adet'
            ELSE r.unit_of_measure
        END AS unit_norm,
        SUM(r.quantity) AS matched_qty
    FROM raw.raw_stock_movements r
    JOIN open_orders_with_next o
      ON o.h_id = r.ref_hid
     AND o.stock_adi = r.material_name
     AND o.unit_norm = CASE
            WHEN r.unit_of_measure = 'PktAdtMt' THEN 'Mt'
            WHEN r.unit_of_measure = 'PktAdt' THEN 'Adet'
            ELSE r.unit_of_measure
        END
     AND r.transaction_date >= o.transaction_date
    WHERE r.ref_hid IS NOT NULL
      AND r.document_type LIKE 'Depo Giri%'
      AND r.company_code = 'WAREHOUSE22'
    GROUP BY
        r.ref_hid,
        r.material_name,
        CASE
            WHEN r.unit_of_measure = 'PktAdtMt' THEN 'Mt'
            WHEN r.unit_of_measure = 'PktAdt' THEN 'Adet'
            ELSE r.unit_of_measure
        END
),
open_orders_enriched AS (
    SELECT
        o.*,
        COALESCE(m.matched_qty, 0) AS matched_qty,
        GREATEST(o.open_qty - COALESCE(m.matched_qty, 0), 0) AS residual_open
    FROM open_orders_with_next o
    LEFT JOIN matched_receipts m
      ON m.h_id = o.h_id
     AND m.stock_adi = o.stock_adi
     AND m.unit_norm = o.unit_norm
),
open_orders_net AS (
    SELECT
        o.stock_adi,
        o.unit_norm,
        SUM(o.residual_open) AS in_transit_qty
    FROM open_orders_enriched o
    GROUP BY o.stock_adi, o.unit_norm
)
SELECT
    v.bom_material_name,
    v.stock_adi,
    cs.warehouse,
    cs.current_stock,
    cs.stock_uom,
    CASE
        WHEN cs.warehouse = 'WAREHOUSE22'
        THEN COALESCE(o.in_transit_qty, 0)
        ELSE NULL
    END AS open_order_in_transit
FROM variants v
JOIN core.raw_current_stock cs
      ON cs.stock_adi = v.stock_adi
JOIN allowed_warehouses aw
      ON aw.warehouse = cs.warehouse
LEFT JOIN open_orders_net o
      ON o.stock_adi = v.stock_adi
     AND o.unit_norm = CASE
            WHEN cs.stock_uom = 'PktAdtMt' THEN 'Mt'
            WHEN cs.stock_uom = 'PktAdt' THEN 'Adet'
            ELSE cs.stock_uom
        END
ORDER BY
    v.bom_material_name,
    cs.warehouse,
    v.stock_adi;

DROP TABLE IF EXISTS core.dashboard_material_overview_new;

CREATE TABLE core.dashboard_material_overview_new AS
WITH mapped AS (
    SELECT DISTINCT bom_material_name
    FROM core.bom_to_stock_map
),
w22 AS (
    SELECT
        bom_material_name,
        SUM(current_stock) AS w22_stock
    FROM core.current_stock_by_variant
    WHERE warehouse = 'WAREHOUSE22'
    GROUP BY bom_material_name
),
fabric_variant_stock AS (
    SELECT
        v.bom_material_name,
        SUM(
            CASE
                WHEN v.stock_uom ILIKE '%%mt%%'
                 AND v.stock_uom NOT ILIKE '%%mt2%%'
                THEN
                    CASE
                        WHEN sm.ek_2 IS NOT NULL
                         AND NULLIF(REGEXP_REPLACE(sm.ek_2, '[^0-9\.]', '', 'g'), '') IS NOT NULL
                        THEN v.current_stock * (
                            NULLIF(REGEXP_REPLACE(sm.ek_2, '[^0-9\.]', '', 'g'), '')::NUMERIC
                            / 100.0
                        )
                        ELSE NULL
                    END
                ELSE v.current_stock
            END
        ) AS fabric_stock_m2
    FROM core.dashboard_material_variants_new v
    LEFT JOIN raw.stock_master sm
      ON sm.adi = v.stock_adi
    WHERE v.warehouse = 'WAREHOUSE22'
    GROUP BY v.bom_material_name
),
fabric_missing_ek2 AS (
    SELECT
        v.bom_material_name
    FROM core.dashboard_material_variants_new v
    LEFT JOIN raw.stock_master sm
      ON sm.adi = v.stock_adi
    WHERE v.warehouse = 'WAREHOUSE22'
      AND v.stock_uom ILIKE '%%mt%%'
      AND v.stock_uom NOT ILIKE '%%mt2%%'
      AND (
            sm.ek_2 IS NULL
         OR NULLIF(REGEXP_REPLACE(sm.ek_2, '[^0-9\.]', '', 'g'), '') IS NULL
      )
    GROUP BY v.bom_material_name
)
SELECT
    b.material_name AS bom_material_name,
    b.unit_of_measure,
    b.material_category,
    f.chosen_method,
    f.forecast_12w,
    COALESCE(
        CASE
            WHEN b.material_category = 'KUMAŞ' THEN COALESCE(fvs.fabric_stock_m2, w.w22_stock)
            ELSE w.w22_stock
        END,
        0
    ) AS current_stock,
    CASE
        WHEN b.material_category = 'KUMAŞ' AND fme.bom_material_name IS NOT NULL THEN 'EN_BILGISI_EKSIK'
        WHEN COALESCE(
            CASE
                WHEN b.material_category = 'KUMAŞ' THEN COALESCE(fvs.fabric_stock_m2, w.w22_stock)
                ELSE w.w22_stock
            END,
            0
        ) < f.forecast_12w THEN 'CRITICAL'
        WHEN COALESCE(
            CASE
                WHEN b.material_category = 'KUMAŞ' THEN COALESCE(fvs.fabric_stock_m2, w.w22_stock)
                ELSE w.w22_stock
            END,
            0
        ) < f.forecast_12w * 1.3 THEN 'MEDIUM'
        ELSE 'SAFE'
    END AS safety_status
FROM core.bom_unique_materials b
JOIN mapped m
      ON m.bom_material_name = b.material_name
LEFT JOIN core.final_forecast_summary f
      ON f.bom_material_name = b.material_name
LEFT JOIN w22 w
      ON w.bom_material_name = b.material_name
LEFT JOIN fabric_variant_stock fvs
      ON fvs.bom_material_name = b.material_name
LEFT JOIN fabric_missing_ek2 fme
      ON fme.bom_material_name = b.material_name;

DROP TABLE IF EXISTS core.dashboard_material_flow_observation_new;

CREATE TABLE core.dashboard_material_flow_observation_new AS
WITH
/* ---------------------------------------------------
   1) Şirket genelinde son koltuk depo sayımı
--------------------------------------------------- */
global_last_seat_count AS (
    SELECT MAX(count_end_date) AS last_count_date
    FROM core.current_stock_seat_warehouses
),

/* ---------------------------------------------------
   2) WAREHOUSE22 çıkışları (dashboard’daki varyantlar için)
--------------------------------------------------- */
w22_outflows AS (
    SELECT
        sm.material_name      AS stock_adi,
        sm.unit_of_measure    AS w22_out_uom,
        SUM(sm.quantity)      AS w22_out_qty
    FROM raw.raw_stock_movements sm
    CROSS JOIN global_last_seat_count g
    WHERE sm.company_code = 'WAREHOUSE22'
      AND sm.document_type = 'Depo Çıkış'
      AND sm.transaction_date > g.last_count_date
      AND EXISTS (
          SELECT 1
          FROM core.dashboard_material_variants_new v
          WHERE v.stock_adi = sm.material_name
      )
    GROUP BY
        sm.material_name,
        sm.unit_of_measure
),

/* ---------------------------------------------------
   3) BOM tüketimi (seçilen BOM’lar için)
--------------------------------------------------- */
seat_bom_consumption AS (
    SELECT
        b.material_name       AS bom_material_name,
        b.unit_of_measure     AS seat_consumed_uom,
        SUM(b.quantity)       AS seat_consumed_qty
    FROM raw.raw_bom_consumption b
    CROSS JOIN global_last_seat_count g
    WHERE b.transaction_date > g.last_count_date
    GROUP BY
        b.material_name,
        b.unit_of_measure
)

/* ---------------------------------------------------
   4) FINAL SNAPSHOT (ilk yapı korunur)
--------------------------------------------------- */
SELECT
    v.bom_material_name,
    v.stock_adi,
    v.warehouse,
    v.stock_uom,
    v.current_stock,

    /* W22 çıkış akışı */
    w.w22_out_qty,
    w.w22_out_uom,

    /* BOM tüketimi */
    c.seat_consumed_qty,
    c.seat_consumed_uom,

    g.last_count_date AS count_end_date

FROM core.dashboard_material_variants_new v
CROSS JOIN global_last_seat_count g

LEFT JOIN w22_outflows w
  ON w.stock_adi = v.stock_adi

LEFT JOIN seat_bom_consumption c
  ON c.bom_material_name = v.bom_material_name

WHERE v.warehouse = 'WAREHOUSE22';

DROP INDEX IF EXISTS core.idx_flow_obs_bom_new;
DROP INDEX IF EXISTS core.idx_flow_obs_stock_new;
DROP INDEX IF EXISTS core.idx_flow_obs_wh_new;

CREATE INDEX idx_flow_obs_bom_new
ON core.dashboard_material_flow_observation_new (bom_material_name);

CREATE INDEX idx_flow_obs_stock_new
ON core.dashboard_material_flow_observation_new (stock_adi);

CREATE INDEX idx_flow_obs_wh_new
ON core.dashboard_material_flow_observation_new (warehouse);

DO $$
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS core.dashboard_material_overview_old';
    EXECUTE 'DROP TABLE IF EXISTS core.dashboard_material_variants_old';
    EXECUTE 'DROP TABLE IF EXISTS core.dashboard_material_flow_observation_old';

    IF to_regclass('core.dashboard_material_overview') IS NOT NULL THEN
        EXECUTE 'ALTER TABLE core.dashboard_material_overview RENAME TO dashboard_material_overview_old';
    END IF;
    IF to_regclass('core.dashboard_material_variants') IS NOT NULL THEN
        EXECUTE 'ALTER TABLE core.dashboard_material_variants RENAME TO dashboard_material_variants_old';
    END IF;
    IF to_regclass('core.dashboard_material_flow_observation') IS NOT NULL THEN
        EXECUTE 'ALTER TABLE core.dashboard_material_flow_observation RENAME TO dashboard_material_flow_observation_old';
    END IF;

    EXECUTE 'ALTER TABLE core.dashboard_material_overview_new RENAME TO dashboard_material_overview';
    EXECUTE 'ALTER TABLE core.dashboard_material_variants_new RENAME TO dashboard_material_variants';
    EXECUTE 'ALTER TABLE core.dashboard_material_flow_observation_new RENAME TO dashboard_material_flow_observation';

    EXECUTE 'DROP TABLE IF EXISTS core.dashboard_material_overview_old';
    EXECUTE 'DROP TABLE IF EXISTS core.dashboard_material_variants_old';
    EXECUTE 'DROP TABLE IF EXISTS core.dashboard_material_flow_observation_old';
END $$;
