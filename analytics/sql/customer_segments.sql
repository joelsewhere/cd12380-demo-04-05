SELECT
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name)  AS customer_name,
    c.loyalty_tier,
    c.state                                 AS customer_state,
    c.joined_date,
    COUNT(o.order_id)                       AS order_count,
    ROUND(SUM(o.declared_total), 2)         AS total_spend,
    ROUND(AVG(o.declared_total), 2)         AS avg_order_value,
    MIN(o.order_date)                       AS first_order_date,
    MAX(o.order_date)                       AS last_order_date
FROM iceberg.transactions.orders    o
JOIN iceberg.transactions.customers c ON o.customer_id = c.customer_id
GROUP BY
    c.customer_id, c.first_name, c.last_name,
    c.loyalty_tier, c.state, c.joined_date