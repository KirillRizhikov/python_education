-- Задание:
--
-- Написать 3 представления для таблицы products;
CREATE OR REPLACE VIEW not_in_stock as
    SELECT product_id, product_little
FROM "Product"
WHERE in_stock = 0;
SELECT * FROM not_in_stock;

CREATE OR REPLACE VIEW top_price as
SELECT product_id, product_little, price
FROM "Product"
ORDER BY price DESC
LIMIT 5;
SELECT * FROM top_price;

CREATE OR REPLACE VIEW category_1 as
SELECT product_id, product_little, category_id
FROM "Product"
WHERE category_id = 1
ORDER BY product_id;
SELECT * FROM category_1;


-- для таблиц order_status и order;
CREATE OR REPLACE VIEW accepted_order as
    SELECT 'Number of accepted orders: ' || count(status_name) as Accepted
FROM "Orders"
JOIN "Order_status" Os on Os.order_status_id = "Orders".order_status_order_status_id
WHERE status_name = 'Accepted';
SELECT * FROM accepted_order;

CREATE OR REPLACE VIEW total_paid as
    SELECT 'Total paid: ' || sum(total) as Paid
FROM "Orders"
JOIN "Order_status" Os on Os.order_status_id = "Orders".order_status_order_status_id
WHERE status_name = 'Paid';
SELECT * FROM total_paid;

CREATE OR REPLACE VIEW order_stat AS
    SELECT status_name, SUM(total)
        FROM shop_db.public."Order_status"
            RIGHT JOIN "Orders" O on "Order_status".order_status_id = O.order_status_order_status_id
    GROUP BY status_name;

SELECT * FROM order_stat;



-- для таблиц products и category;
CREATE OR REPLACE VIEW product_category_1 AS
    SELECT product_id, C.category_id , category_description
FROM "Product"
JOIN "Categories" C on C.category_id = "Product".category_id
WHERE category_title =  'Category 1'
ORDER BY product_id;

EXPLAIN
SELECT * FROM product_category_1;

CREATE OR REPLACE VIEW cat_1_total AS
    SELECT "Categories".category_id, sum(price * in_stock) as total
        FROM "Categories"
            LEFT JOIN "Product" P on "Categories".category_id = P.category_id
GROUP BY "Categories".category_id
ORDER BY category_id;

EXPLAIN
SELECT * FROM cat_1_total;


DROP VIEW category_stat;
CREATE OR REPLACE VIEW category_stat AS
    SELECT "Categories".category_id, count(product_id)
        FROM "Categories"
            LEFT JOIN "Product" P on "Categories".category_id = P.category_id
    GROUP BY "Categories".category_id
    ORDER BY category_id;

SELECT * FROM category_stat;


-- Создать материализированное представление для "тяжелого" запроса на свое усмотрение;
CREATE MATERIALIZED VIEW IF NOT EXISTS top_5_users AS
    SELECT first_name, count(cart_id) as card_num, count(order_id) as order_num
FROM "Users"
LEFT JOIN "Carts" ON user_id = "Carts".users_user_id
Left Join "Orders" ON "Carts".cart_id = "Orders".carts_cart_id
WHERE cart_id is not NULL
GROUP BY user_id
ORDER BY order_num desc
LIMIT 5;

EXPLAIN
SELECT * FROM top_5_users;

SELECT COUNT(category_id) as "Cat.1 tot number"
FROM product_category_1;

DROP VIEW category_stat;


