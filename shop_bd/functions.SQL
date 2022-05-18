-- Задание:
-- 1. Создать функцию, которая сетит shipping_total = 0 в таблице order,
--     если город юзера равен x (аргумент функции),
--     и возвращает сумму всех заказов для поля shipping_total.
--     Использовать IF clause.


CREATE OR REPLACE FUNCTION find_city(
                                    search_user_id int,
                                    search_city varchar
                                    )
returns "Orders".total%type

LANGUAGE plpgsql
AS $$
DECLARE
    temp_row record;
    user_total "Orders".total%type;
BEGIN
    user_total = 0;
	for temp_row in (SELECT order_id, cart_id, user_id, city, "Orders".shipping_total, "Orders".total
            FROM "Orders"
            LEFT JOIN "Carts" on "Orders".carts_cart_id = cart_id
            LEFT JOIN "Users" ON user_id = "Carts".users_user_id
            WHERE city = search_city and user_id = search_user_id)
        LOOP user_total := user_total + temp_row.total;
            UPDATE "Orders"
            SET shipping_total = 0
	        WHERE order_id = temp_row.order_id;
        END LOOP;
    	if not found then
            raise 'City id % or user % is not found', search_city, search_user_id;
        end if;
    RETURN user_total;
END ; $$;

SELECT * FROM find_city(1,'city 1');



-- 2. Написать 3 любые (НО осмысленные) хранимые процедуры с использованием условий,
--     циклов и транзакций. Дать комментарии что делает каждая процедура;

CREATE OR REPLACE PROCEDURE product_description(search_product_id int, description text)
LANGUAGE plpgsql
AS $$
--     changes description of a specific product
BEGIN
    UPDATE "Product"
    SET product_description = description
    WHERE product_id = search_product_id;
    IF NOT FOUND THEN
        RAISE 'Product id % not found', search_product_id;
    end if;
end; $$;

CALL product_description(2, 'New description of the product');



CREATE OR REPLACE PROCEDURE discount_product_category(search_category text, discount float)
LANGUAGE plpgsql
AS $$
    declare
    total_price float;
    total_discount float;

--     reduces prices of the products in the given category according to the discount.
--         If total discount is higher than 10000 rejects transaction
BEGIN
    discount := discount/100;
    UPDATE "Product"
    SET price = "Product".price*(1.0-discount)
    FROM (
    SELECT product_id, "Product".price, category_title
    FROM "Product"
    JOIN "Categories" C on C.category_id = "Product".category_id
    WHERE category_title = search_category)
    AS temp_table
    WHERE temp_table.product_id = "Product".product_id;

    SELECT sum("Product".price)
    FROM "Product"
    JOIN "Categories" C on C.category_id = "Product".category_id
    WHERE category_title = search_category
        INTO total_price;
        total_discount := total_price*(1.0/(1-discount)-1);
        IF total_discount < 10000 THEN
            COMMIT;
        ELSE
            RAISE 'Discount is too big';
        end if;
END; $$;

CALL discount_product_category('Category 4', 50.0);


CREATE OR REPLACE PROCEDURE check_profit()
LANGUAGE plpgsql
AS $$
    DECLARE
        temp_row record;
        total_paid float;
        total_canceled float;
        ratio float;

BEGIN
-- Raises ration between sum of canceled and paid orders

    total_canceled := 0;
    total_paid := 0;
	for temp_row in (SELECT order_id, order_status_order_status_id, "Orders".total, status_name
            FROM "Orders"
            JOIN "Order_status" ON "Orders".order_status_order_status_id = "Order_status".order_status_id
            WHERE status_name in ('Paid', 'Canceled'))
        LOOP
            IF temp_row.status_name = 'Paid' THEN total_paid:= total_paid + temp_row.total;
            ELSE total_canceled:= total_canceled + temp_row.total;
            END IF;
        end loop;
	        ratio := total_canceled/total_paid;
            RAISE 'total_canceled/total_paid = ratio %' , ratio;
end; $$;

call check_profit();
