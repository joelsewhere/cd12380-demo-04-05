SELECT
    p.category,
    SUBSTR(o.order_date, 1, 7)                                           AS year_month,
    COUNT(DISTINCT o.order_id)                                           AS order_count,
    SUM(i.quantity)                                                      AS items_sold,
    ROUND(SUM(i.line_total), 2)                                          AS total_revenue,
    ROUND(SUM(i.line_total) / NULLIF(COUNT(DISTINCT o.order_id), 0), 2)  AS avg_order_value
FROM transactions.orders       o
JOIN transactions.order_items  i ON o.order_id   = i.order_id
JOIN transactions.products     p ON i.product_id = p.product_id
GROUP BY p.category, SUBSTR(o.order_date, 1, 7)