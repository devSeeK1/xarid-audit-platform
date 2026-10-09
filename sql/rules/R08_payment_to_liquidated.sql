-- R08. Faoliyati tugatilgan (likvidatsiya qilingan) korxonaga to'lov.
SELECT 'R08', 'payment', p.payment_id, c.buyer_id, c.supplier_stir,
       p.amount AS amount_at_risk,
       to_json({'contract_id': c.contract_id, 'liquidation_date': c.liquidation_date,
                'payment_date': p.payment_date})::VARCHAR
FROM dwh.fact_payment p
JOIN dwh.v_contracts c USING (contract_id)
WHERE c.liquidation_date IS NOT NULL
  AND p.payment_date > c.liquidation_date
