-- R05. Affillangan yetkazuvchilar: direktori (PINFL xeshi) yoki yuridik manzili bir xil
-- bo'lgan bir nechta korxona bitta buyurtmachida shartnoma yutgan.
WITH links AS (
    SELECT director_key AS link_key, 'direktor' AS link_type, stir
    FROM dwh.dim_supplier WHERE director_key IS NOT NULL
    UNION ALL
    SELECT address_norm, 'manzil', stir
    FROM dwh.dim_supplier WHERE address_norm IS NOT NULL
), groups AS (
    SELECT link_key, link_type FROM links GROUP BY ALL HAVING COUNT(*) >= 2
), wins AS (
    SELECT g.link_type, g.link_key, c.buyer_id,
           list_sort(list(DISTINCT c.supplier_stir)) AS stirs,
           COUNT(*) AS n_contracts, SUM(c.amount) AS total
    FROM groups g
    JOIN links l USING (link_key, link_type)
    JOIN dwh.v_contracts c ON c.supplier_stir = l.stir
    GROUP BY ALL
    HAVING COUNT(DISTINCT c.supplier_stir) >= 2
)
SELECT 'R05', 'supplier_cluster',
       buyer_id || '|' || array_to_string(stirs, ','),
       buyer_id, stirs[1],
       total AS amount_at_risk,
       to_json({'link_type': link_type, 'suppliers': stirs, 'contracts': n_contracts,
                'total': round(total, 2)})::VARCHAR
FROM wins
QUALIFY row_number() OVER (PARTITION BY buyer_id, stirs ORDER BY link_type = 'direktor' DESC) = 1
