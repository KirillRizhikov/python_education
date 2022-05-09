-- Task 1
CREATE TABLE Public."Order_status"(
    order_status_id int,
    status_name VARCHAR(255),
    PRIMARY KEY (order_status_id)
                                  );

CREATE TABLE Public."Categories"(
    category_id int,
    category_title VARCHAR(255),
    category_description text,
    PRIMARY KEY (category_id)
);

CREATE TABLE Public."Users"(
    user_id int,
    email VARCHAR(255),
    user_password VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    middle_name VARCHAR(255),
    is_staff int2,
    country VARCHAR(255),
    city VARCHAR(255),
    address text,
    PRIMARY KEY (user_id)
);



CREATE TABLE Public."Product"(
    product_id int,
    product_little VARCHAR(255),
    product_description text,
    in_stock int,
    price FLOAT,
    slug VARCHAR(45),
    category_id int,
    PRIMARY KEY (product_id),
    FOREIGN KEY (category_id) REFERENCES Public."Categories" (category_id)
);

CREATE TABLE Public."Carts"(
    cart_id int,
    users_user_id int,
    subtotal DECIMAL,
    total DECIMAL,
    timestam timestamp,
    PRIMARY KEY (cart_id),
    FOREIGN KEY (users_user_id) REFERENCES Public."Users" (user_id)
    );

CREATE TABLE Public."Orders"(
    order_id int,
    carts_cart_id int,
    order_status_order_status_id int,
    shipping_total DECIMAL,
    total DECIMAL,
    created_at timestamp,
    updated_at timestamp,
    PRIMARY KEY (order_id),
    FOREIGN KEY (carts_cart_id) REFERENCES Public."Carts" (cart_id),
    FOREIGN KEY (order_status_order_status_id) REFERENCES Public."Order_status" (order_status_id)
        );

CREATE TABLE Public."Cart_product"(
    carts_cart_id int,
    products_product_id int,
    FOREIGN KEY (carts_cart_id) REFERENCES Public."Carts" (cart_id),
    FOREIGN KEY (products_product_id) REFERENCES Public."Product" (product_id)
);

-- sudo docker cp ./cart_products.csv shop_bd_db_1:/usr/src/cart_products.csv
-- sudo docker cp ./carts.csv shop_bd_db_1:/usr/src/carts.csv
-- sudo docker cp ./categories.csv shop_bd_db_1:/usr/src/categories.csv
-- sudo docker cp ./orders.csv shop_bd_db_1:/usr/src/orders.csv
-- sudo docker cp ./order_statuses.csv shop_bd_db_1:/usr/src/order_statuses.csv
-- sudo docker cp ./products.csv shop_bd_db_1:/usr/src/products.csv
-- sudo docker cp ./users.csv shop_bd_db_1:/usr/src/users.csv

COPY Public."Order_status" FROM '/usr/src/order_statuses.csv' DELIMITER ',' CSV;

COPY Public."Categories" FROM '/usr/src/categories.csv' DELIMITER ',' CSV;

COPY Public."Users" FROM '/usr/src/users.csv' DELIMITER ',' CSV

COPY Public."Product" FROM '/usr/src/products.csv' DELIMITER ',' CSV;

COPY Public."Carts" FROM '/usr/src/carts.csv' DELIMITER ',' CSV;

COPY Public."Orders" FROM '/usr/src/orders.csv' DELIMITER ',' CSV;

COPY Public."Cart_product" FROM '/usr/src/cart_products.csv' DELIMITER ',' CSV;

-- Task 2
ALTER TABLE Public."Users"
ADD phone_number int NULL ;

ALTER TABLE Public."Users"
    ALTER COLUMN phone_number TYPE VARCHAR(255);

-- Task 3
UPDATE Public."Product" SET price = price * 2;

