SELECT
    order_date,
    SUBSTR(order_date, 1, 7)         AS year_month,
    COUNT(order_id)                  AS order_count,
    COUNT(DISTINCT customer_id)      AS unique_customers,
    ROUND(SUM(declared_total), 2)    AS daily_revenue,
    ROUND(AVG(declared_total), 2)    AS avg_order_value
FROM transactions.orders
GROUP BY order_date, SUBSTR(order_date, 1, 7)
ORDER BY order_date