SELECT cu.customer_id AS id,
    cu.first_name || ' ' || cu.last_name AS name,
    a.address,
    a.postal_code AS "zip code",
    a.phone,
    city.city,
    country.country,
        CASE
            WHEN cu.activebool THEN 'active'
            ELSE ''
        END AS notes,
    cu.store_id AS sid
   FROM customer cu
     INNER JOIN address a USING (address_id)
     INNER JOIN city USING (city_id)
     INNER JOIN country USING (country_id);

CREATE OR REPLACE VIEW customer_master AS
  SELECT cu.customer_id AS id,
    cu.first_name || ' ' || cu.last_name AS name,
    a.address,
    a.postal_code AS "zip code",
    a.phone,
    city.city,
    country.country,
        CASE
            WHEN cu.activebool THEN 'active'
            ELSE ''
        END AS notes,
    cu.store_id AS sid
   FROM customer cu
     INNER JOIN address a USING (address_id)
     INNER JOIN city USING (city_id)
     INNER JOIN country USING (country_id);

SELECT table_name
FROM INFORMATION_SCHEMA.views
WHERE table_schema = ANY(current_schemas(false));

SELECT * FROM customer_master;

ALTER VIEW customer_master RENAME TO customer_info;

DROP VIEW IF EXISTS customer_info;

CREATE VIEW film_master
AS
SELECT
	film_id,
	title,
	release_year,
	length,
	name category
FROM
	film
INNER JOIN film_category
	USING (film_id)
INNER JOIN category
	USING(category_id);

CREATE VIEW horror_film
AS
SELECT
	film_id,
	title,
	release_year,
	length
FROM
	film_master
WHERE
	category = 'Horror';

CREATE VIEW comedy_film
AS
SELECT
	film_id,
	title,
	release_year,
	length
FROM
	film_master
WHERE
	category = 'Comedy';

CREATE VIEW film_category_stat
AS
SELECT
	name,
	COUNT(film_id)
FROM category
INNER JOIN film_category USING (category_id)
INNER JOIN film USING (film_id)
GROUP BY name;

CREATE VIEW film_length_stat
AS
SELECT
	name,
	SUM(length) film_length
FROM category
INNER JOIN film_category USING (category_id)
INNER JOIN film USING (film_id)
GROUP BY name;

DROP VIEW comedy_film;

DROP VIEW film_master;

DROP VIEW film_master CASCADE;

DROP VIEW film_length_stat, film_category_stat;

DROP VIEW usa_cities;
CREATE VIEW usa_cities AS SELECT
	city,
	country_id
FROM
	city
WHERE
	country_id = 103;

SELECT * FROM usa_cities;

INSERT INTO usa_cities (city, country_id)
VALUES('San Jose', 103);

SELECT
	city,
	country_id
FROM
	city
WHERE
	country_id = 103
ORDER BY
	last_update DESC;

DELETE
FROM
	usa_cities
WHERE
	city = 'San Jose';

CREATE VIEW usa_city AS SELECT
	city_id,
	city,
	country_id
FROM
	city
WHERE
	country_id = 103
ORDER BY
	city;

SELECT * FROM usa_city;

INSERT INTO usa_city (city, country_id)
VALUES ('Birmingham', 102);
INSERT INTO usa_city (city, country_id)
VALUES ('Cambridge', 102);

CREATE
OR REPLACE VIEW usa_city AS SELECT
	city_id,
	city,
	country_id
FROM
	city
WHERE
	country_id = 103
ORDER BY
	city WITH CHECK OPTION;

UPDATE usa_city
SET country_id = 102
WHERE
	city_id = 135;

CREATE VIEW city_a AS SELECT
	city_id,
	city,
	country_id
FROM
	city
WHERE
	city LIKE 'A%';

CREATE
OR REPLACE VIEW city_a_usa AS SELECT
	city_id,
	city,
	country_id
FROM
	city_a
WHERE
	country_id = 103
WITH CASCADED CHECK OPTION;

INSERT INTO city_a_usa (city, country_id)
VALUES ('Houston', 103);

CREATE OR REPLACE VIEW city_a_usa AS SELECT
	city_id,
	city,
	country_id
FROM
	city_a
WHERE
	country_id = 103
WITH LOCAL CHECK OPTION;

INSERT INTO city_a_usa (city, country_id)
VALUES ('Houston', 103);
SELECT * FROM city WHERE city = 'Houston';

CREATE MATERIALIZED VIEW rental_by_category
AS
 SELECT c.name AS category,
    sum(p.amount) AS total_sales
   FROM (((((payment p
     JOIN rental r ON ((p.rental_id = r.rental_id)))
     JOIN inventory i ON ((r.inventory_id = i.inventory_id)))
     JOIN film f ON ((i.film_id = f.film_id)))
     JOIN film_category fc ON ((f.film_id = fc.film_id)))
     JOIN category c ON ((fc.category_id = c.category_id)))
  GROUP BY c.name
  ORDER BY sum(p.amount) DESC
WITH NO DATA;

SELECT * FROM rental_by_category;

REFRESH MATERIALIZED VIEW rental_by_category;

CREATE UNIQUE INDEX rental_category ON rental_by_category (category);

REFRESH MATERIALIZED VIEW CONCURRENTLY rental_by_category;



DROP TABLE employees;

CREATE TABLE employees (
	employee_id serial PRIMARY KEY,
	full_name VARCHAR NOT NULL,
	manager_id INT
);

INSERT INTO employees (
	employee_id,
	full_name,
	manager_id
)
VALUES
	(1, 'Michael North', NULL),
	(2, 'Megan Berry', 1),
	(3, 'Sarah Berry', 1),
	(4, 'Zoe Black', 1),
	(5, 'Tim James', 1),
	(6, 'Bella Tucker', 2),
	(7, 'Ryan Metcalfe', 2),
	(8, 'Max Mills', 2),
	(9, 'Benjamin Glover', 2),
	(10, 'Carolyn Henderson', 3),
	(11, 'Nicola Kelly', 3),
	(12, 'Alexandra Climo', 3),
	(13, 'Dominic King', 3),
	(14, 'Leonard Gray', 4),
	(15, 'Eric Rampling', 4),
	(16, 'Piers Paige', 7),
	(17, 'Ryan Henderson', 7),
	(18, 'Frank Tucker', 8),
	(19, 'Nathan Ferguson', 8),
	(20, 'Kevin Rampling', 8);

WITH RECURSIVE subordinates AS (
	SELECT
		employee_id,
		manager_id,
		full_name
	FROM
		employees
	WHERE
		employee_id = 2
	UNION
		SELECT
			e.employee_id,
			e.manager_id,
			e.full_name
		FROM
			employees e
		INNER JOIN subordinates s ON s.employee_id = e.manager_id
) SELECT
	*
FROM
	subordinates;

WITH RECURSIVE reporting_line AS (
	SELECT
		employee_id,
		full_name AS subordinates
	FROM
		employees
	WHERE
		manager_id IS NULL
	UNION ALL
		SELECT
			e.employee_id,
			(
				rl.subordinates || ' > ' || e.full_name
			) AS subordinates
		FROM
			employees e
		INNER JOIN reporting_line rl ON e.manager_id = rl.employee_id
) SELECT
	employee_id,
	subordinates
FROM
	reporting_line
ORDER BY
	employee_id;

CREATE RECURSIVE VIEW reporting_line (employee_id, subordinates) AS
SELECT
	employee_id,
	full_name AS subordinates
FROM
	employees
WHERE
	manager_id IS NULL
UNION ALL
	SELECT
		e.employee_id,
		(
			rl.subordinates || ' > ' || e.full_name
		) AS subordinates
	FROM
		employees e
	INNER JOIN reporting_line rl ON e.manager_id = rl.employee_id;

SELECT
	subordinates
FROM
	reporting_line
WHERE
	employee_id = 10;