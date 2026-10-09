-- R01. Xaridni bo'lib-bo'lib amalga oshirish (tender chegarasidan qochish).
-- Bitta buyurtmachi bitta yetkazuvchidan bir xil toifadagi tovarni qisqa muddat ichida
-- chegaradan past bir nechta to'g'ridan-to'g'ri shartnoma bilan sotib olgan,
-- jami summa esa chegaradan oshib ketgan. Faqat chegaraga YAQIN shartnomalar hisobga olinadi
-- (summa >= chegara × split_min_share) — bu tasodifiy kichik xaridlarni filtrlaydi.
WITH d AS (
    SELECT * FROM dwh.v_contracts
    WHERE method IN ('togridan_togri', 'elektron_dokon')
      AND amount < {direct_purchase_limit}
      AND amount >= {direct_purchase_limit} * {split_min_share}
), w AS (
    SELECT *,
           COUNT(*)          OVER win AS n_in_window,
           SUM(amount)       OVER win AS total_in_window,
           MAX(contract_date) OVER win AS window_end
    FROM d
    WINDOW win AS (PARTITION BY buyer_id, supplier_stir, category ORDER BY contract_date
                   RANGE BETWEEN CURRENT ROW AND INTERVAL {split_window_days} DAY FOLLOWING)
)
SELECT 'R01' AS rule_code,
       'contract_group' AS entity_type,
       buyer_id || '|' || supplier_stir || '|' || category AS entity_id,
       buyer_id, supplier_stir,
       total_in_window AS amount_at_risk,
       to_json({'category': category, 'window_start': contract_date, 'window_end': window_end,
                'contracts': n_in_window, 'total': round(total_in_window, 2),
                'limit': {direct_purchase_limit}})::VARCHAR AS details
FROM w
WHERE n_in_window >= {split_min_count} AND total_in_window > {direct_purchase_limit}
QUALIFY row_number() OVER (PARTITION BY buyer_id, supplier_stir, category
                           ORDER BY total_in_window DESC) = 1
