-- 1. Сравнить цену каждого продукта n с средней ценой продуктов в категории
-- продукта n. Использовать window function. Таблица результата должна
-- содержать такие колонки: category_title, product_title, price, avg;

SELECT
    category_title,
    product_title,
    price,
    AVG(price) OVER (
        PARTITION BY category_title
        ) as avarege
FROM "Categories"
LEFT JOIN "Product" ON "Product".category_id = "Categories".category_id
ORDER BY "Product".category_id;


-- 2. Добавить 2 любых триггера и обработчика к ним.
-- Снабдить комментариями - что делают триггеры и обработчики.

DROP table price_log;
CREATE TABLE price_log
(
    id               INT GENERATED ALWAYS AS IDENTITY,
    price_product_id int NOT NULL,
    new_price FLOAT,
    change_time      timestamp   NOT NULL,
    PRIMARY KEY (id)
);

INSERT INTO price_log(price_product_id,new_price,change_time)
SELECT product_id, price, now()
FROM "Product";


-- Adds new product price with date to the  log_price_change table
CREATE OR REPLACE FUNCTION log_price_change()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS
$$
BEGIN
    IF NEW.price <> OLD.price THEN
        INSERT INTO price_log(price_product_id, new_price, change_time)
        VALUES (NEW.product_id, NEW.price, now() );
    end if;
	RETURN NEW;
END;$$;


-- Triggers on update of Product table
CREATE TRIGGER price_change
  BEFORE UPDATE
  ON "Product"
  FOR EACH ROW
  EXECUTE PROCEDURE log_price_change();

-- Check
UPDATE "Product"
SET price = 789
WHERE product_id = 12;


-- Status is changed to paid then reduces number of products and increases price by 2%
CREATE OR REPLACE FUNCTION price_adjustment()
RETURNS TRIGGER
LANGUAGE plpgsql
AS
$$
BEGIN
    IF NEW.order_status_order_status_id = 3 AND
       OLD.order_status_order_status_id !=3 THEN
        UPDATE "Product"
        SET price = "Product".price *1.02, in_stock = in_stock-1
            FROM (
            SELECT "Product".price, product_id
            FROM
            "Orders"
            JOIN "Cart_product" ON "Cart_product".carts_cart_id = "Orders".carts_cart_id
            JOIN "Product" ON "Cart_product".Products_product_id = "Product".product_id
            WHERE order_id = NEW.order_id)
            AS temp_table
            WHERE temp_table.product_id = "Product".product_id;

    end if;
    RETURN NEW;
end;
$$;

-- Triggers on update of Orders table
CREATE TRIGGER order_status_update
  BEFORE UPDATE
  ON "Orders"
  FOR EACH ROW
  EXECUTE PROCEDURE price_adjustment();


-- Check
UPDATE "Orders"
SET order_status_order_status_id = 3
WHERE order_id = 3;

SELECT price, product_id, "Cart_product".carts_cart_id, in_stock
            FROM
            "Orders"
            JOIN "Cart_product" ON "Cart_product".carts_cart_id = "Orders".carts_cart_id
            JOIN "Product" ON "Cart_product".Products_product_id = "Product".product_id
            WHERE order_id = 3
