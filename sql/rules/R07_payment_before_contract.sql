-- R07. Shartnoma tuzilishidan OLDIN amalga oshirilgan to'lov.
SELECT 'R07', 'payment', p.payment_id, c.buyer_id, c.supplier_stir,
       p.amount AS amount_at_risk,
       to_json({'contract_id': c.contract_id, 'payment_date': p.payment_date,
                'contract_date': c.contract_date,
                'days_before': date_diff('day', p.payment_date, c.contract_date)})::VARCHAR
FROM dwh.fact_payment p
JOIN dwh.v_contracts c USING (contract_id)
WHERE p.payment_date < c.contract_date
