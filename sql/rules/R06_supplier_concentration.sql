-- R06. Bitta yetkazuvchining buyurtmachi yillik xarajatidagi ulushi haddan tashqari yuqori.
WITH s AS (
    SELECT buyer_id, year(contract_date) AS yr, supplier_stir,
           SUM(amount) AS amt, COUNT(*) AS n
    FROM dwh.v_contracts GROUP BY ALL
), t AS (
    SELECT buyer_id, yr, SUM(amt) AS total, SUM(n) AS n_total FROM s GROUP BY ALL
)
SELECT 'R06', 'buyer_year_supplier',
       buyer_id || '|' || yr || '|' || supplier_stir,
       buyer_id, supplier_stir,
       s.amt - t.total * {concentration_share} AS amount_at_risk,   -- ruxsat etilgan ulushdan ortig'i
       to_json({'year': yr, 'share_pct': round(100 * s.amt / t.total, 1),
                'supplier_contracts': s.n, 'buyer_contracts': t.n_total})::VARCHAR
FROM s JOIN t USING (buyer_id, yr)
WHERE t.n_total >= {concentration_min_contracts}
  AND s.n >= 3
  AND s.amt / t.total > {concentration_share}
