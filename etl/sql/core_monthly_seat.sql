-- Monthly refresh for seat depots (run on day 2)
-- Updates seat_* tables and replaces seat rows in core.raw_current_stock

CREATE TABLE IF NOT EXISTS core.seat_warehouses (
    warehouse TEXT PRIMARY KEY
);

INSERT INTO core.seat_warehouses (warehouse) VALUES
('JALUZİ KOLTUK DEPO'),
('KATLAMALI KOLTUK DEPO'),
('STOR KOLTUK DEPO')
ON CONFLICT DO NOTHING;

DROP TABLE IF EXISTS core.seat_count_events;

CREATE TABLE core.seat_count_events AS
WITH count_rows AS (
    SELECT
        sm.company_code     AS warehouse,
        sm.transaction_date AS trx_date,
        sm.material_name    AS stock_adi,
        sm.unit_of_measure  AS stock_uom,
        sm.quantity
    FROM raw.raw_stock_movements sm
    JOIN core.seat_warehouses sw
      ON sw.warehouse = sm.company_code
    WHERE sm.document_type = 'Depo Giriş'
),
distinct_days AS (
    SELECT DISTINCT warehouse, trx_date
    FROM count_rows
),
marked AS (
    SELECT
        warehouse,
        trx_date,
        LAG(trx_date) OVER (PARTITION BY warehouse ORDER BY trx_date) AS prev_date
    FROM distinct_days
),
evented AS (
    SELECT
        warehouse,
        trx_date,
        SUM(
            CASE
                WHEN prev_date IS NULL THEN 1
                WHEN trx_date - prev_date > 7 THEN 1
                ELSE 0
            END
        ) OVER (PARTITION BY warehouse ORDER BY trx_date) AS event_id
    FROM marked
)
SELECT
    c.warehouse,
    c.trx_date,
    c.stock_adi,
    c.stock_uom,
    c.quantity,
    e.event_id
FROM count_rows c
JOIN evented e
  ON e.warehouse = c.warehouse
 AND e.trx_date  = c.trx_date;


DROP TABLE IF EXISTS core.seat_last_event;

CREATE TABLE core.seat_last_event AS
SELECT warehouse, MAX(event_id) AS last_event_id
FROM core.seat_count_events
GROUP BY warehouse;


DROP TABLE IF EXISTS core.current_stock_seat_warehouses;

CREATE TABLE core.current_stock_seat_warehouses AS
SELECT
    e.stock_adi,
    e.warehouse,
    e.stock_uom,
    SUM(e.quantity) AS current_stock,
    MAX(e.trx_date) AS count_end_date,
    MIN(e.trx_date) AS count_start_date
FROM core.seat_count_events e
JOIN core.seat_last_event le
  ON le.warehouse = e.warehouse
 AND le.last_event_id = e.event_id
GROUP BY
    e.stock_adi, e.warehouse, e.stock_uom;


CREATE TABLE IF NOT EXISTS core.raw_current_stock (
    stock_adi TEXT,
    warehouse TEXT,
    stock_uom TEXT,
    current_stock NUMERIC
);

DELETE FROM core.raw_current_stock
WHERE warehouse IN (SELECT warehouse FROM core.seat_warehouses);

INSERT INTO core.raw_current_stock (stock_adi, warehouse, stock_uom, current_stock)
SELECT
    stock_adi,
    warehouse,
    stock_uom,
    current_stock
FROM core.current_stock_seat_warehouses;
