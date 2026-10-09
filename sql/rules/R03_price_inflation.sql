-- R03. Birlik narxi bozor medianasidan sezilarli darajada yuqori.
SELECT 'R03', 'contract', c.contract_id, c.buyer_id, c.supplier_stir,
       (c.unit_price - m.median_price) * c.quantity AS amount_at_risk,
       to_json({'category': c.category, 'unit_price': c.unit_price,
                'market_median': round(m.median_price, 2),
                'ratio': round(c.unit_price / m.median_price, 2)})::VARCHAR
FROM dwh.v_contracts c
JOIN dwh.ref_market_price m
  ON m.category = c.category AND m.month = date_trunc('month', c.contract_date)::DATE
WHERE c.unit_price > m.median_price * {price_inflation_ratio}
