-- Задание 1
-- Создайте новую таблицу potential customers с полями id, email, name, surname, second_name, city

CREATE TABLE Public."potential_customers"(
potential_customer_id SERIAL,
potential_customer_email VARCHAR(255),
potential_customer_name VARCHAR(255),
potential_customer_surname VARCHAR(255),
potential_customer_second_name VARCHAR(255),
city VARCHAR(255),
PRIMARY KEY (potential_customer_id)
);

-- Заполните данными таблицу.
INSERT INTO potential_customers(potential_customer_email,
                                potential_customer_name,
                                potential_customer_surname,
                                potential_customer_second_name,
                                city)
    VALUES ('email1@gmail.com',
            'name 1',
            'surname 1',
            'second name 1',
            'city 1');

INSERT INTO potential_customers (potential_customer_email,
                                potential_customer_name,
                                potential_customer_surname,
                                potential_customer_second_name,
                                city)
    SELECT email, 'potential '||first_name, last_name, middle_name, city
        from "Users";


-- Выведите имена и электронную почту потенциальных и существующих пользователей из города city 17
SELECT unnest(array[first_name, potential_customer_name]) as name,
       unnest(array["Users".email, "potential_customers".potential_customer_email]) as email,
       unnest(array["Users".city, "potential_customers".city]) as city
    FROM "Users"
        INNER JOIN "potential_customers" on "Users".city = potential_customers.city
        WHERE "Users".city = 'city 17';


-- Задание 2
-- Вывести имена и электронные адреса всех users отсортированных по городам и по имени (по алфавиту)
SELECT first_name, email
from "Users"
    ORDER BY city, first_name;


-- Задание 3
-- Вывести наименование группы товаров, общее количество по группе товаров в порядке убывания количества
SELECT  category_title, sum(in_stock) as SUM
    FROM "Product"
        INNER JOIN "Categories" on "Product".category_id = "Categories".category_id
        GROUP BY "Categories".category_title
        ORDER BY SUM DESC;


-- Задание 4
-- 1. Вывести продукты, которые ни разу не попадали в корзину.
SELECT product_id, carts_cart_id
FROM "Product"
LEFT JOIN shop_db.public."Cart_product" ON "Product".product_id = "Cart_product".products_product_id
WHERE carts_cart_id is NULL
ORDER BY product_id;


-- 2. Вывести все продукты, которые так и не попали ни в 1 заказ. (но в корзину попасть могли)
SELECT product_id, cart_id, order_id
FROM "Product"
LEFT JOIN "Cart_product" ON "Product".product_id = "Cart_product".products_product_id
LEFT JOIN "Carts" ON "Cart_product".carts_cart_id = "Carts".cart_id
LEFT JOIN "Orders" ON "Carts".cart_id = "Orders".carts_cart_id
WHERE order_id is null
ORDER BY product_id;


-- 3. Вывести топ 10 продуктов, которые добавляли в корзины чаще всего
SELECT  products_product_id, count(products_product_id) as order_number
FROM "Product"
LEFT JOIN "Cart_product" ON "Product".product_id = "Cart_product".products_product_id
WHERE carts_cart_id is not NULL
group by products_product_id
ORDER BY order_number DESC
LIMIT 10;


-- 4. Вывести топ 10 продуктов, которые не только добавляли в корзины, но и оформляли заказы чаще всего
SELECT product_id, count(order_id) as order_number
FROM "Product"
LEFT JOIN "Cart_product" ON "Product".product_id = "Cart_product".products_product_id
LEFT JOIN "Carts" ON "Cart_product".carts_cart_id = "Carts".cart_id
LEFT JOIN "Orders" ON "Carts".cart_id = "Orders".carts_cart_id
WHERE order_id is not NULL
GROUP BY product_id
ORDER BY order_number desc
LIMIT 10;


-- 5. Вывести топ 5 юзеров, которые потратили больше всего денег (total в заказе)
SELECT first_name,count(cart_id) as cart_num, sum(total) as total_sum
FROM "Users"
LEFT JOIN "Carts" ON  user_id = "Carts".users_user_id
    WHERE total is NOT NULL
GROUP BY first_name
ORDER BY total_sum desc
LIMIT 5;


-- 6. Вывести топ 5 юзеров, которые сделали больше всего заказов (кол-во заказов)
SELECT first_name, count(cart_id) as card_num, count(order_id) as order_num
FROM "Users"
LEFT JOIN "Carts" ON user_id = "Carts".users_user_id
Left Join "Orders" ON "Carts".cart_id = "Orders".carts_cart_id
WHERE cart_id is not NULL
GROUP BY user_id
ORDER BY order_num desc
LIMIT 5;


-- 7. Вывести топ 5 юзеров, которые создали корзины, но так и не сделали заказы
SELECT first_name, count(cart_id) as cart_num
FROM "Carts"
LEFT JOIN "Orders" ON "Orders".carts_cart_id = "Carts".cart_id
JOIN "Users" ON "Carts".users_user_id = "Users".user_id
WHERE "Orders".order_id is NULL
GROUP BY first_name
ORDER BY cart_num desc
LIMIT 5;