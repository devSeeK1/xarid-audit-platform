-- GOLD qatlam: "yulduz" (star schema) modeli. {silver} — silver parquet papkasi.
-- Har ishga tushirishda to'liq qayta quriladi (idempotent).

CREATE OR REPLACE TABLE dwh.dim_buyer AS
SELECT row_number() OVER (ORDER BY buyer_id)::INTEGER AS buyer_key,
       buyer_id, buyer_name, sector, region, budget_type
FROM read_parquet('{silver}/buyers.parquet');

CREATE OR REPLACE TABLE dwh.dim_supplier AS
SELECT row_number() OVER (ORDER BY stir)::INTEGER AS supplier_key,
       stir, name, reg_date, region, status, liquidation_date,
       director_key,          -- PINFL emas, balki uning tuzlangan SHA-256 xeshi
       address_norm
FROM read_parquet('{silver}/suppliers.parquet');

CREATE OR REPLACE TABLE dwh.dim_category AS
SELECT row_number() OVER (ORDER BY category)::INTEGER AS category_key, category, any_value(unit) AS unit
FROM read_parquet('{silver}/market_prices.parquet')
GROUP BY category;

CREATE OR REPLACE TABLE dwh.dim_date AS
SELECT strftime(d, '%Y%m%d')::INTEGER AS date_key,
       d::DATE AS date,
       year(d) AS year, quarter(d) AS quarter, month(d) AS month,
       isodow(d) AS iso_dow, isodow(d) IN (6, 7) AS is_weekend
FROM generate_series(DATE '2015-01-01', DATE '2027-12-31', INTERVAL 1 DAY) t(d);

CREATE OR REPLACE TABLE dwh.ref_market_price AS
SELECT category, month, median_price FROM read_parquet('{silver}/market_prices.parquet');

CREATE OR REPLACE TABLE dwh.fact_contract AS
SELECT c.contract_id,
       b.buyer_key, s.supplier_key, k.category_key,
       strftime(c.contract_date, '%Y%m%d')::INTEGER AS contract_date_key,
       c.contract_date,
       c.method, c.quantity, c.unit_price, c.amount
FROM read_parquet('{silver}/contracts.parquet') c
JOIN dwh.dim_buyer b    ON b.buyer_id = c.buyer_id
JOIN dwh.dim_supplier s ON s.stir = c.supplier_stir
JOIN dwh.dim_category k ON k.category = c.category;

CREATE OR REPLACE TABLE dwh.fact_payment AS
SELECT p.payment_id, p.contract_id,
       strftime(p.payment_date, '%Y%m%d')::INTEGER AS payment_date_key,
       p.payment_date, p.amount
FROM read_parquet('{silver}/payments.parquet') p
JOIN dwh.fact_contract c USING (contract_id);

-- Tahlil uchun qulay keng ko'rinish (auditorlar uchun)
CREATE OR REPLACE VIEW dwh.v_contracts AS
SELECT c.contract_id, c.contract_date, c.method, c.quantity, c.unit_price, c.amount,
       b.buyer_id, b.buyer_name, b.sector, b.region AS buyer_region,
       s.stir AS supplier_stir, s.name AS supplier_name, s.reg_date AS supplier_reg_date,
       s.status AS supplier_status, s.liquidation_date, s.director_key, s.address_norm,
       k.category
FROM dwh.fact_contract c
JOIN dwh.dim_buyer b USING (buyer_key)
JOIN dwh.dim_supplier s USING (supplier_key)
JOIN dwh.dim_category k USING (category_key);
