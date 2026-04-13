SELECT * FROM processed LIMIT 10;

SELECT country, sum
FROM processed
ORDER BY sum DESC;

SELECT SUM(count) AS total_orders
FROM processed;