-- Задание:
-- Использовать транзакции для insert, update, delete на 3х таблицах.
-- Предоставить разнообразные примеры включая возврат к savepoints.

-- INSERT
INSERT INTO "Categories"(category_id, category_title, category_description)
VALUES (21, 'Category 21', 'Description 21');

INSERT INTO "Categories"(category_id, category_title, category_description)
VALUES (22, 'Category 21', 'Description 22'),
       (23, 'Category 21', 'Description 23');

SELECT *
FROM "Categories";

BEGIN;
UPDATE "Categories"
    SET category_title =  'new ' || category_title
WHERE category_id = 21;
COMMIT;


SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN;
SELECT category_id, category_title, category_description
FROM
    "Categories";
INSERT INTO "Categories"
VALUES (24, 'Category 24', 'Description 24')
ON CONFLICT (category_id)
DO NOTHING ;
SAVEPOINT my_point;
INSERT INTO "Categories"
VALUES (25, 'Category 25', 'Description 25');
INSERT INTO "Categories"
VALUES (26, 'Category 26', 'Description 26');
ROLLBACK TO SAVEPOINT my_point;
COMMIT;

BEGIN;
DELETE FROM "Categories"
WHERE category_id = 24;
COMMIT;

SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

DROP VIEW view_lastproduct_id;
CREATE VIEW view_lastproduct_id AS
    SELECT MAX(product_id)
        FROM "Product";
BEGIN;
SAVEPOINT beginning_save_point;
SELECT *
    FROM "Product";

select * from view_lastproduct_id;

INSERT INTO "Product"(product_id,
                      product_little,
                      product_description,
                      in_stock,
                      price,
                      slug,
                      category_id)
VALUES (((select * from view_lastproduct_id)+1),
        'Product'||(select * from view_lastproduct_id),
        'Product description',
    1,
    123.4,
    'Product',
    1);
-- ROLLBACK TO SAVEPOINT beginning_save_point;
RELEASE SAVEPOINT beginning_save_point;
UPDATE "Product"
    SET price = price * 2
WHERE product_id = (select * from view_lastproduct_id);
COMMIT ;

SELECT *
FROM "Product"
ORDER BY product_id DESC;

DELETE FROM "Categories"
WHERE category_id = (select * from view_lastproduct_id);

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SET TRANSACTION ISOLATION LEVEL read committed ;

BEGIN;
SAVEPOINT start_point;
INSERT INTO shop_db.public."Order_status"(order_status_id, status_name)
VALUES (6, 'Suspended');

UPDATE shop_db.public."Order_status"
SET status_name = CASE
    WHEN status_name = 'Suspended' THEN
        'Pending'
        ELSE status_name
END ;

DELETE FROM "Order_status"
WHERE order_status_id = 6;
COMMIT;

DELETE FROM "Order_status"
WHERE order_status_id = 6;

SELECT * FROM shop_db.public."Order_status";

ROLLBACK;

SELECT *
FROM shop_db.public."Order_status";