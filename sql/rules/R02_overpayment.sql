-- R02. Shartnoma summasidan ortiqcha to'lov.
WITH paid AS (
    SELECT contract_id, SUM(amount) AS paid, COUNT(*) AS n_payments
    FROM dwh.fact_payment GROUP BY contract_id
)
SELECT 'R02', 'contract', c.contract_id, c.buyer_id, c.supplier_stir,
       p.paid - c.amount AS amount_at_risk,
       to_json({'contract_amount': c.amount, 'paid': round(p.paid, 2),
                'overpaid_pct': round(100 * (p.paid / c.amount - 1), 1),
                'payments': p.n_payments})::VARCHAR
FROM dwh.v_contracts c
JOIN paid p USING (contract_id)
WHERE p.paid > c.amount * (1 + {overpayment_tolerance})
