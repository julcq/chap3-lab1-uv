Exercice 1 : What is the average number of orders per user, and the average quantity per order?

SELECT
  count(*)::DOUBLE / count(DISTINCT user_uuid) AS avg_orders_per_user,
  avg(quantity) AS avg_quantity_per_order
FROM orders;

result : avg_orders_per_user = 56.58
         avg_quantity_per_order = 3.05

Exercice 2 : For each user, what is the date of their first and last order, and the number of days between them?

SELECT
  user_uuid,
  min(date) AS first_order,
  max(date) AS last_order,
  date_diff('day', min(date), max(date)) AS days_between
FROM orders
GROUP BY user_uuid
ORDER BY user_uuid;

Exercice 3 : Which hour of the day has the highest quantity sold? Use the hour function.

SELECT hour(date) AS hour, sum(quantity) AS quantity
FROM orders
GROUP BY hour
ORDER BY quantity DESC
LIMIT 1;

Exercice 4 : For each product, what is the month-over-month variation of the quantity sold, in percent? Use the lag window function.

WITH monthly AS (
  SELECT
    strftime(date, '%Y-%m') AS month,
    product,
    sum(quantity)::DOUBLE AS quantity
  FROM orders
  GROUP BY month, product
)
SELECT
  month,
  product,
  quantity,
  round(100 * (quantity - lag(quantity) OVER w) / lag(quantity) OVER w, 1) AS variation_pct
FROM monthly
WINDOW w AS (PARTITION BY product ORDER BY month)
ORDER BY product, month;

Exercice 5 : Which users ordered every product at least once?

SELECT u.username, u.name
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL
HAVING count(DISTINCT o.product) = (SELECT count(DISTINCT product) FROM orders)
ORDER BY u.username;