Задание:
1. Создать функцию, которая сетит shipping_total = 0 в таблице order,
    если город юзера равен x (аргумент функции),
    и возвращает сумму всех заказов для поля shipping_total.
    Использовать IF clause.

2. Написать 3 любые (НО осмысленные) хранимые процедуры с использованием условий,
    циклов и транзакций. Дать комментарии что делает каждая процедура;


SELECT order_id, cart_id, first_name, city, C.total, shipping_total
FROM "Orders"
LEFT JOIN "Carts" C on "Orders".carts_cart_id = C.cart_id
LEFT JOIN "Users" ON user_id = C.users_user_id
WHERE city LIKE 'city 1';

CREATE OR REPLACE PROCEDURE shipping(search_city varchar)
LANGUAGE plpgsql
AS $$
    BEGIN
        UPDATE "Orders"
        SET shipping_total = 0
        FROM (
            SELECT order_id, cart_id, first_name, city, shipping_total
            FROM "Orders"
            LEFT JOIN "Carts" on "Orders".carts_cart_id = cart_id
            LEFT JOIN "Users" ON user_id = "Carts".users_user_id
            WHERE city = search_city)
            AS temp_table
        WHERE "Orders".order_id = temp_table.order_id;
    COMMIT;
END; $$;

call shipping('city 3');

DROP FUNCTION find_cit(search_city varchar);

CREATE OR REPLACE FUNCTION find_cit(search_city varchar)
    returns table (
	city_user_id "Users".user_id%type,
	city_shipping_tot "Orders".shipping_total%type)
LANGUAGE plpgsql
AS $$
    BEGIN

        return query
	SELECT user_id, sum(shipping_total)
            FROM "Orders"
            LEFT JOIN "Carts" on "Orders".carts_cart_id = cart_id
            LEFT JOIN "Users" ON user_id = "Carts".users_user_id
            WHERE city = search_city
            GROUP BY user_id;
end; $$;

select * from find_cit('city 4');


CREATE OR REPLACE FUNCTION find_cit_1(search_city varchar)
    returns table (
	city_user_id "Users".user_id%type,
	city_shipping_tot "Orders".shipping_total%type)
LANGUAGE plpgsql
AS $$
    BEGIN

        return query
	        SELECT user_id, sum(shipping_total)
            FROM "Orders"
            LEFT JOIN "Carts" on "Orders".carts_cart_id = cart_id
            LEFT JOIN "Users" ON user_id = "Carts".users_user_id
            WHERE city = search_city
            GROUP BY user_id;
          if not found then
                raise 'City id % not found', search_city;
          end if;

        UPDATE "Orders"
        SET shipping_total = 0
        FROM (
            SELECT order_id, cart_id, first_name, city, shipping_total
            FROM "Orders"
            LEFT JOIN "Carts" on "Orders".carts_cart_id = cart_id
            LEFT JOIN "Users" ON user_id = "Carts".users_user_id
            WHERE city = search_city)
            AS temp_table
        WHERE "Orders".order_id = temp_table.order_id;

end; $$;


select * from find_cit_1('city 5');

