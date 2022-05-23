-- Для всех нижеприведенных заданий используй субд PostgreSql и созданную ER диаграмму в первом задании по БД "DB ER Diagram". Все команды сохранить в отдельные файлы для каждого задания.
--
-- Задание 1
-- Создать бд, создать таблицы, добавить связи между таблицами.

CREATE TABLE Manufacturer(
    manufacturer_id INT GENERATED ALWAYS AS IDENTITY,
    manufacturer VARCHAR(255),
    PRIMARY KEY (manufacturer_id)
                                  );


CREATE TABLE Model
(
    model_id INT GENERATED ALWAYS AS IDENTITY,
    car_model VARCHAR(255),
    manufacturer_id INT,
    PRIMARY KEY (model_id),
    FOREIGN KEY (manufacturer_id) REFERENCES Manufacturer (manufacturer_id)
);

CREATE TABLE Car
(
    car_id INT GENERATED ALWAYS AS IDENTITY,
    car_number VARCHAR(255),
    model_id INT,
    PRIMARY KEY (car_id),
    FOREIGN KEY (model_id) REFERENCES Model(model_id)
);


CREATE TABLE City(
    city_id INT GENERATED ALWAYS AS IDENTITY,
    city VARCHAR(255),
    PRIMARY KEY (city_id)
                                  );
ALTER TABLE City
ADD COLUMN state_name VARCHAR;

CREATE TABLE Address
(
    address_id INT GENERATED ALWAYS AS IDENTITY,
    address VARCHAR(255),
    city_id INT,
    PRIMARY KEY (address_id),
    FOREIGN KEY (city_id) REFERENCES City(city_id)
);

CREATE TABLE Customer
(
    customer_id INT GENERATED ALWAYS AS IDENTITY,
    customer_name VARCHAR(255),
    customer_phone VARCHAR(100),
    address_id INT,
    PRIMARY KEY (customer_id),
    FOREIGN KEY (address_id) REFERENCES Address(address_id)
);

CREATE TABLE Branch
(
    branch_id INT GENERATED ALWAYS AS IDENTITY,
    branch_name VARCHAR(255),
    branch_phone VARCHAR(100),
    address_id INT,
    PRIMARY KEY (branch_id),
    FOREIGN KEY (address_id) REFERENCES Address(address_id)
);

CREATE TABLE public.Order
(
    order_id INT GENERATED ALWAYS AS IDENTITY,
    date_of_ranting date,
    period date,
    customer_id INT,
    branch_id INT,
    car_id INT,
    PRIMARY KEY (order_id),
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id),
    FOREIGN KEY (branch_id) REFERENCES Branch(branch_id),
    FOREIGN KEY (car_id) REFERENCES Car(car_id)
);

-- Заполнить таблицу данными
--     (их нужно сгенерировать в большом количестве - для этого можно использовать последовательности,
--     собственные или встроенные функции - никаких внешних генераторов);

CREATE TABLE us_cities
(city VARCHAR,
city_ascii VARCHAR,
state_id VARCHAR,
state_name VARCHAR
);

COPY us_cities FROM '/usr/src/uscities_clean.csv' DELIMITER ',' CSV HEADER ;



INSERT INTO city(city, state_name)
SELECT city_ascii, state_name
FROM us_cities
GROUP BY city_ascii, state_name;


SELECT count(city)
FROM city;

SELECT count(city)
FROM us_cities;


INSERT INTO Address(address, city_id)
SELECT md5(random()::text) ||', '||md5(random()::text) , random_between(1,30351)
from generate_series(1, 50000) AS s(id);

TRUNCATE TABLE City RESTART IDENTITY CASCADE ;

UPDATE Address
set address = state_id || ' ' ||Address.address
from  City
JOIN us_cities ON City.state_name = us_cities.state_name
WHERE Address.city_id = City.city_id;

    SELECT count(*)
FROM Address;

CREATE OR REPLACE FUNCTION random_between(low INT ,high INT)
   RETURNS INT AS
$$
BEGIN
   RETURN floor(random()* (high-low + 1) + low);
END;
$$ language 'plpgsql' STRICT;




CREATE TABLE cars_import
(year int,
manufacturer VARCHAR,
model VARCHAR,
body_style VARCHAR
);
COPY cars_import FROM '/usr/src/cars.csv' DELIMITER ',' CSV HEADER ;


INSERT INTO Manufacturer(manufacturer)
SELECT distinct manufacturer
FROM cars_import;

INSERT INTO Model(car_model, manufacturer_id)
SELECT model, manufacturer_id
FROM cars_import
join Manufacturer on cars_import.manufacturer = manufacturer.manufacturer;

SELECT count(*)
    FROM model;
TRUNCATE TABLE Car RESTART IDENTITY CASCADE ;

INSERT INTO Car(car_number, model_id)
SELECT substring(md5(random()::text),1,8), random_between(1,375)
from generate_series(1, 50000) ;



INSERT INTO Customer(customer_name, customer_phone,address_id)
SELECT 'Customer name '|| cust, '+1-'||random_between(100000000,999999999), random_between(1,50000)
from generate_series(1, 10000) as cust;

select count(*)
from customer;

INSERT INTO Branch(branch_name, branch_phone,address_id)
SELECT 'Branch '|| br, '+1-'||random_between(100000000,999999999), random_between(1,50000)
from generate_series(1, 1000) as br;

select count(*)
from car;

ALTER table public.Order
ALTER COLUMN period SET DATA TYPE int using period::INTEGER;

INSERT INTO public.Order(price, date_of_ranting, period, customer_id, car_id, branch_id)
SELECT
    random_between(2,8),
    timestamp '2014-01-10 20:00:00' + random() *
                                      (timestamp '2014-01-20 20:00:00' -
                                       timestamp '2022-01-10 10:00:00'),
    random_between(2,50),
    random_between(1,10000),
    random_between(1,50000),
    random_between(1,1000)
from generate_series(1, 100000);


-- Задание 2
-- Придумать 3 различных запроса SELECT с осмысленным использованием разных видов JOIN.
-- Используя explain добавить только необходимые индексы для уменьшения стоимости (cost) запросов.


-- Select top 5 users with that spent more money
EXPLAIN ANALYSE
SELECT customer_name, sum(price*period) as total, count(order_id) as trip_number
    FROM Customer
        LEFT JOIN public.Order O on Customer.customer_id = O.customer_id
        GROUP BY customer_name
        HAVING sum(price*period) > 3000
        ORDER BY total DESC ;

-- Gets most called car of same manufacturer in a single city
EXPLAIN ANALYSE
SELECT City.city, Manufacturer.manufacturer, count(order_id)
    FROM public.Order
    JOIN Car C on "order".car_id = C.car_id
    JOIN Model Mo on C.model_id = Mo.model_id
    JOIN Manufacturer ON Mo.manufacturer_id = Manufacturer.manufacturer_id
    JOIN Branch ON "order".branch_id = Branch.branch_id
    JOIN Address ON Branch.address_id = Address.address_id
    JOIN City ON Address.city_id = City.city_id
GROUP BY City.city, Manufacturer.manufacturer
ORDER BY count(order_id) DESC;

CREATE EXTENSION pg_trgm;
CREATE INDEX trgm_idx_car_number ON Car USING  gin (car_number gin_trgm_ops);
DROP INDEX trgm_idx_car_number;


-- Finds car number and order details
EXPLAIN ANALYSE
SELECT Car.car_id, car_number, order_id, customer_name, date_of_ranting
FROM Car
RIGHT JOIN public.Order ON Car.car_id = public.Order.car_id
Join Customer ON public.Order.customer_id = Customer.customer_id
WHERE car_number LIKE '%adbe%'
ORDER BY car_number;


CREATE INDEX  ON public.Order(order_id);
CREATE INDEX  ON public.Order(price);
CREATE INDEX  ON public.Order(period);
CREATE INDEX  ON public.Order(period,price);
CREATE INDEX  ON Customer(customer_id);
CREATE INDEX  ON Customer(customer_name);
SET work_mem TO '4MB';


-- Задание 3
-- Создать 3 представления (1 из них должно быть материализированным и хранить данные от "тяжелого" запроса).

DROP VIEW top_5_cars;

-- Top  rented models
CREATE OR REPLACE VIEW top_5_cars AS
    SELECT Model.car_model, count(order_id) as order_number
FROM Model
LEFT JOIN Car C on Model.model_id = C.model_id
LEFT JOIN public.Order O on C.car_id = O.car_id
GROUP BY Model.car_model
ORDER BY order_number DESC
LIMIT 5;

SELECT * FROM top_5_cars;

-- Most used car
CREATE OR REPLACE VIEW most_used_car AS
SELECT car_number, sum(period) as total_time
FROM Car
LEFT JOIN public.Order O on Car.car_id = O.car_id
WHERE period is not NULL
GROUP BY car_number
ORDER BY total_time DESC
LIMIT 10;

SELECT * FROM most_used_car;


-- most profitable states
CREATE MATERIALIZED VIEW IF NOT EXISTS state_profit AS
SELECT City.state_name, sum(period*price) as profit
    FROM public.Order
    JOIN Branch ON "order".branch_id = Branch.branch_id
    JOIN Address ON Branch.address_id = Address.address_id
    JOIN City ON Address.city_id = City.city_id
GROUP BY City.state_name
ORDER BY profit DESC
LIMIT 5;

SELECT * FROM state_profit;


-- Задание 4
-- Создать 3 функции (одна из них должна возвращать таблицу,

-- Returns list of cars of given model
DROP FUNCTION get_car_model;
CREATE OR REPLACE FUNCTION get_car_model (search_car_model varchar)
	RETURNS TABLE(
		search_model VARCHAR,
		car_numbers VARCHAR
	)
	LANGUAGE plpgsql
AS $$
BEGIN
	RETURN QUERY
		SELECT car_model, car_number
		FROM Model
		LEFT JOIN Car ON Model.model_id = Car.model_id
	    WHERE car_model ilike search_car_model;
END;$$;

SELECT * FROM get_car_model ('Camr%');

--     одна из них должна использовать циклы,



-- Gets all customer in the same city as given branch

CREATE OR REPLACE FUNCTION get_branch_city_customer(search_branch VARCHAR)
RETURNS TABLE (
    search_city VARCHAR,
    search_customer VARCHAR
              )
LANGUAGE plpgsql
AS $$
    DECLARE
        temp_row record;
        find_city VARCHAR;
BEGIN
        SELECT city
        FROM Branch
        JOIN Address ON Branch.address_id = Address.address_id
        JOIN City ON Address.city_id = City.city_id
        WHERE branch_name = search_branch
        INTO find_city;

        FOR temp_row in(
            SELECT customer_name, customer_phone
            FROM Customer
            JOIN Address ON Customer.address_id = Address.address_id
            JOIN City ON Address.city_id = City.city_id
            WHERE City.city = find_city)
        LOOP search_city := search_branch || ' '|| upper(find_city);
            search_customer :=  upper(temp_row.customer_name) || ' Phone: ' ||temp_row.customer_phone;
            return next;
        END LOOP ;
END;$$;

SELECT * FROM get_branch_city_customer('Branch 766');

-- Check
SELECT city, count(customer_name), branch_name
FROM Address
LEFT JOIN Customer ON Address.address_id = Customer.address_id
LEFT JOIN Branch ON Address.address_id = Branch.address_id
JOIN City ON Address.city_id = City.city_id
WHERE customer_name IS NOT NULL and branch_name IS NOT NULL
GROUP BY branch_name, City.city
ORDER BY count(customer_name) DESC ;

drop function get_cars_from_model(search_manufact VARCHAR);
--     одна из них должна использовать курсор).
-- Cursors
CREATE OR REPLACE FUNCTION get_cars_from_model(search_manufact VARCHAR)
RETURNS TABLE (
    s_manufact VARCHAR,
    s_cars VARCHAR)
LANGUAGE plpgsql
    AS $$
    DECLARE
        rec_car record;
        cur_cars cursor(search_manufact VARCHAR)
        FOR SELECT manufacturer,car_model, car_number
        FROM Manufacturer
        JOIN Model ON Manufacturer.manufacturer_id = Model.manufacturer_id
        JOIN Car ON Model.model_id = Car.model_id
        WHERE Manufacturer.manufacturer = search_manufact;
BEGIN
OPEN cur_cars(search_manufact);
      LOOP
      fetch cur_cars into rec_car;
      exit when not found;
        s_manufact := rec_car.manufacturer || ' ' || rec_car.car_model;
        s_cars := rec_car.car_number;
        return next;
        end loop;
       close cur_cars;
end; $$;

SELECT * from get_cars_from_model('Ford');


-- Задание 5
-- Написать 2 любые хранимые процедуры.
--     В них использовать транзакции для insert, update, delete.


CREATE OR REPLACE PROCEDURE discount(s_customer VARCHAR)
LANGUAGE plpgsql
AS $$
    declare
    total_discount float;

-- Calculates discount as 1/1000 of total spent money
BEGIN
    SELECT sum(price*O.period)/1000
    FROM public.Order as O
    JOIN Customer ON O.customer_id = Customer.customer_id
    WHERE customer_name = s_customer
    INTO total_discount;

    IF total_discount > 0.3 THEN
    total_discount := 0.3;
    end if;

    UPDATE public.Order as O
    SET price = O.price * (1-total_discount)
    FROM (
    SELECT price, O.customer_id
    FROM public.Order as O
    JOIN Customer ON O.customer_id = Customer.customer_id
    WHERE customer_name = s_customer
         ) as temp_table
    WHERE temp_table.customer_id = O.customer_id;
END; $$;

CALL discount('Customer name 1');


-- Checks if exists and deletes city from the table City
CREATE OR REPLACE PROCEDURE remove_city(s_city VARCHAR)
    LANGUAGE plpgsql
    AS $$
    BEGIN
IF (SELECT exists(SELECT 1 FROM City WHERE City.city = s_city))
   THEN
    ALTER TABLE City DISABLE TRIGGER ALL;
    DELETE  FROM City
    WHERE city = s_city;
    ALTER TABLE City ENABLE TRIGGER ALL;
    COMMIT;
    ELSE
    RAISE 'No such city';
end if;
END; $$;

call remove_city('Augusta');

SELECT city, customer_name
    FROM City
        LEFT JOIN Address A on City.city_id = A.city_id
LEFT JOIN Customer ON A.address_id = Customer.address_id;


-- Задание 6
-- Добавить 2 триггера (один из них ДО операции по изменению данных, второй после) и функции
--     или процедуры-обработчики к ним.

-- Checks new period value
CREATE OR REPLACE FUNCTION min_period()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS
$$
BEGIN
    IF NEW.period <> OLD.period and NEW.period < 2  THEN
        RAISE 'period cant be less than 1';
    end if;
	RETURN NEW;
END;$$;


-- Triggers on update of Order
CREATE TRIGGER period_change
  BEFORE UPDATE
  ON public.Order
  FOR EACH ROW
  EXECUTE PROCEDURE min_period();

UPDATE public.Order
SET period = 1
WHERE order_id = 1;


-- Checks new period value and updates price
CREATE OR REPLACE FUNCTION update_price()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS
$$
BEGIN
    IF NEW.period > OLD.period and NEW.period > 20   THEN
        UPDATE public.Order as O
        SET price = price * 0.8
        WHERE O.order_id = new.order_id;
    end if;
	RETURN NEW;
END;$$;


-- Triggers on update of Order
CREATE TRIGGER update_price
  AFTER UPDATE
  ON public.Order
  FOR EACH ROW
  EXECUTE PROCEDURE update_price();

-- Check
UPDATE public.Order
SET period = 30
WHERE order_id = 1;