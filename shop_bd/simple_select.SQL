
-- Задание 1
-- Вывести:

-- 1. всех юзеров,
SELECT *
FROM "Users";

-- 2. все продукты,
SELECT *
FROM "Product";

-- 3. все статусы заказов
SELECT *
FROM "Order_status";

-- Задание 2
-- Вывести заказы, которые успешно доставлены и оплачены
SELECT order_id, status_name
FROM "Orders"
INNER JOIN "Order_status" Os on Os.order_status_id = "Orders".order_status_order_status_id
WHERE status_name ='Finished'
or status_name ='Paid';



-- Задание 3
-- 1. Продукты, цена которых больше 80.00 и меньше или равно 150.00
SELECT *
FROM "Product"
WHERE price > 80
AND price <= 150;

-- 2. заказы совершенные после 01.10.2020 (поле created_at)
SELECT *
FROM "Orders"
WHERE created_at > '10-01-2020';

-- 3. заказы полученные за первое полугодие 2020 года
SELECT *
FROM "Orders"
WHERE created_at > '01-01-2020'
AND created_at < '07-01-2020';

SELECT *
FROM "Orders"
WHERE created_at BETWEEN '01-01-2020' AND '06-30-2020';

-- 4. подукты следующих категорий Category 7, Category 11, Category 18
SELECT *
FROM "Product"
WHERE category_id = 7
or category_id = 11
or category_id = 18;

SELECT *
FROM "Product"
WHERE category_id in (7, 11, 18);

-- 5. незавершенные заказы по состоянию на 31.12.2020
SELECT *
FROM "Orders"
WHERE order_status_order_status_id = 2
AND updated_at < '12-31-2020';

-- 6.Вывести все корзины, которые были созданы, но заказ так и не был оформлен.
SELECT cart_id, status_name
FROM "Orders"
INNER JOIN "Carts" on cart_id = "Orders".carts_cart_id
INNER JOIN "Order_status" on order_status_id = "Orders".order_status_order_status_id
WHERE status_name = 'Canceled';

-- Задание 4
-- Вывести:
-- 1. среднюю сумму всех завершенных сделок
SELECT AVG(total)
FROM "Orders"
INNER JOIN "Order_status" on order_status_id = "Orders".order_status_order_status_id
WHERE status_name = 'Finished';

-- 2. вывести максимальную сумму сделки за 3 квартал 2020
SELECT MAX(total)
FROM "Orders"
INNER JOIN "Order_status" on order_status_id = "Orders".order_status_order_status_id
WHERE status_name = 'Finished'
AND updated_at BETWEEN '09-01-2020' AND '12-31-2020';