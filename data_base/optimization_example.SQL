eXPLAIN (COSTS FALSE) SELECT
    *
FROM
    film
WHERE
    film_id = 100;

EXPLAIN SELECT COUNT(*) FROM film;

EXPLAIN ANALYZE
SELECT
    f.film_id,
    title,
    name category_name
FROM
    film f
    INNER JOIN film_category fc
        ON fc.film_id = f.film_id
    INNER JOIN category c
        ON c.category_id = fc.category_id
ORDER BY
    title;


CREATE TEMPORARY TABLE temp_table (
    int_column INT,
    text_column TEXT);
INSERT INTO temp_table
  SELECT i, md5(random()::text)
  FROM generate_series(1, 1000000) AS i;

EXPLAIN SELECT * FROM temp_table;

INSERT INTO temp_table
  SELECT i, md5(random()::text)
  FROM generate_series(1, 10) AS i;
EXPLAIN SELECT * FROM temp_table;

ANALYZE temp_table;
EXPLAIN SELECT * FROM temp_table;

EXPLAIN (ANALYZE) SELECT * FROM temp_table;
EXPLAIN (ANALYZE,BUFFERS) SELECT * FROM temp_table;
EXPLAIN (ANALYZE,BUFFERS) SELECT * FROM temp_table;

CREATE INDEX ON temp_table(int_column);
EXPLAIN SELECT * FROM temp_table WHERE int_column > 500;

SET enable_seqscan TO off;
EXPLAIN (ANALYZE) SELECT * FROM temp_table WHERE int_column > 500;
SET enable_seqscan TO on;
EXPLAIN SELECT * FROM temp_table WHERE int_column < 500;
EXPLAIN SELECT * FROM temp_table
        WHERE int_column < 500 AND text_column LIKE 'abcd%';
EXPLAIN (ANALYZE)
SELECT * FROM temp_table WHERE text_column LIKE 'abcd%';

CREATE INDEX ON temp_table(text_column);
EXPLAIN (ANALYZE) SELECT * FROM temp_table
WHERE text_column LIKE 'abcd%';

CREATE INDEX ON temp_table(text_column text_pattern_ops);
EXPLAIN (ANALYZE) SELECT * FROM temp_table
WHERE text_column LIKE 'abcd%';

DROP INDEX temp_table_text_column_idx;
EXPLAIN (ANALYZE) SELECT * FROM temp_table ORDER BY text_column;
EXPLAIN (ANALYZE,BUFFERS) SELECT * FROM temp_table ORDER BY text_column;
EXPLAIN (ANALYZE,BUFFERS)
SELECT * FROM temp_table WHERE text_column LIKE 'ab%' LIMIT 10;


CREATE TEMPORARY TABLE another_temp_table (some_int integer, some_bool boolean);
INSERT INTO another_temp_table
  SELECT i, i%2=1
  FROM generate_series(1, 500000) AS i;
ANALYZE another_temp_table;

EXPLAIN (ANALYZE)
SELECT * FROM temp_table JOIN another_temp_table
    ON temp_table.int_column=another_temp_table.some_int;

CREATE INDEX ON another_temp_table(some_int);
EXPLAIN (ANALYZE)
SELECT * FROM temp_table JOIN another_temp_table
    ON temp_table.int_column=another_temp_table.some_int;

EXPLAIN (ANALYZE)
SELECT * FROM temp_table LEFT JOIN another_temp_table
    ON temp_table.int_column=another_temp_table.some_int;

SET enable_seqscan TO off;
EXPLAIN (ANALYZE)
SELECT * FROM temp_table LEFT JOIN another_temp_table
    ON temp_table.int_column=another_temp_table.some_int;
SET enable_seqscan TO on;

SET work_mem TO '15MB';
SET enable_seqscan TO ON;
EXPLAIN (ANALYZE)
SELECT * FROM temp_table LEFT JOIN another_temp_table
    ON temp_table.int_column=another_temp_table.some_int;

SET work_mem TO '15MB';
SET work_mem TO '30MB';
SET enable_indexscan TO off;
EXPLAIN (ANALYZE)
SELECT * FROM temp_table LEFT JOIN another_temp_table
    ON temp_table.int_column=another_temp_table.some_int;
SET enable_indexscan TO on;
SET work_mem TO '4MB';