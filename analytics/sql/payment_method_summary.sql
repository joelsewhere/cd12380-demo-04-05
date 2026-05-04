WITH totals AS (
    SELECT SUM(declared_total) AS grand_total
    FROM iceberg.transactions.orders
)
SELECT
    o.payment_method,
    COUNT(o.order_id) AS txn_count,
    ROUND(SUM(o.declared_total), 2) AS total_revenue,
    ROUND(AVG(o.declared_total), 2) AS avg_value,
    ROUND(SUM(o.declared_total) / t.grand_total * 100, 2) AS revenue_pct
FROM iceberg.transactions.orders o
CROSS JOIN totals t
GROUP BY o.payment_method, t.grand_total
ORDER BY total_revenue DESC