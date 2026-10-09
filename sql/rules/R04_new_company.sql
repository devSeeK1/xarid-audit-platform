-- R04. Yaqinda ro'yxatdan o'tgan korxona bilan yirik shartnoma.
SELECT 'R04', 'contract', contract_id, buyer_id, supplier_stir,
       amount AS amount_at_risk,
       to_json({'supplier_reg_date': supplier_reg_date, 'contract_date': contract_date,
                'days_since_registration': date_diff('day', supplier_reg_date, contract_date),
                'amount': amount})::VARCHAR
FROM dwh.v_contracts
WHERE date_diff('day', supplier_reg_date, contract_date) < {new_company_days}
  AND amount >= {new_company_min_amount}
