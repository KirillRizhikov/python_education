-- Придумать 3 сложных запроса с использованием 2 и более таблиц, которые можно оптимизировать
-- с помощью индекса (проверять стоит ли оптимизировать запрос оператором explain) и оптимизировать их
-- используя индекс. Результат проверять также операторами explain, analyze. Желательно
-- в финальный SQL-файл вставить вывод планировщика в виде комментариев.
--
-- Все запросы включить в итоговый SQL-файл.

EXPLAIN ANALYSE
SELECT product_id, count(order_id) as order_number
FROM "Product"
LEFT JOIN "Cart_product" ON "Product".product_id = "Cart_product".products_product_id
LEFT JOIN "Carts" ON "Cart_product".carts_cart_id = "Carts".cart_id
LEFT JOIN "Orders" ON "Carts".cart_id = "Orders".carts_cart_id
WHERE order_id is not NULL
GROUP BY product_id
ORDER BY order_number desc
LIMIT 10;

CREATE INDEX Product_product_id_idx ON "Product"(product_id);
CREATE INDEX Cart_cart_id_idx ON "Carts"(cart_id);

DROP INDEX "Product_product_id_idx";
SET work_mem TO '15MB';
CREATE INDEX ON "Cart_product"(carts_cart_id);
CREATE INDEX ON "Cart_product"(products_product_id);

CREATE INDEX ON "Cart_product"(carts_cart_id, products_product_id) ;
CREATE INDEX ON "Order_status"(status_name);
CREATE INDEX ON "Carts"(cart_id);
CREATE INDEX  ON "Product"(price);


EXPLAIN ANALYSE
SELECT product_id, carts_cart_id, status_name
    FROM "Product"
        LEFT JOIN "Cart_product" ON product_id = products_product_id
        LEFT JOIN "Carts" ON carts_cart_id  = cart_id
        LEFT JOIN "Order_status" ON cart_id = carts_cart_id
WHERE price > 100
ORDER BY product_id;


INSERT INTO "Cart_product"(carts_cart_id, products_product_id)
SELECT random_between(1, 2000), random_between(1, 4000)
from generate_series(1, 100000)
ORDER BY random();

CREATE OR REPLACE function random_between(low int, hight int)
RETURNS int as
    $$
    BEGIN
    RETURN floor(random()*(hight-low+1)+ low);
    END ;
    $$ language plpgsql;



UPDATE "Users"
    SET phone_number =  md5(random()::text)
WHERE phone_number IS NULL;

EXPLAIN
SELECT *
FROM perf_test
WHERE reason LIKE 'bc%' AND annotation LIKE 'ab%';


CREATE INDEX trgm_idx_user_phone ON "Users" USING  gin (phone_number gin_trgm_ops);
DROP INDEX trgm_idx_user_phone;

EXPLAIN ANALYSE
SELECT first_name, count(cart_id) as card_num, count(order_id) as order_num, phone_number
FROM "Users"
LEFT JOIN "Carts" ON user_id = "Carts".users_user_id
Left Join "Orders" ON "Carts".cart_id = "Orders".carts_cart_id
WHERE phone_number LIKE '%adb%'
GROUP BY user_id
ORDER BY order_num desc
LIMIT 5;


--
-- -------------------------------------
--
-- Default settings
-- Memory 100kB
-- Limit  (cost=1531.48..1531.50 rows=10 width=12) (actual time=27.841..27.847 rows=10 loops=1)
--   ->  Sort  (cost=1531.48..1541.48 rows=4001 width=12) (actual time=27.839..27.844 rows=10 loops=1)
-- "        Sort Key: (count(""Orders"".order_id)) DESC"
--         Sort Method: top-N heapsort  Memory: 25kB
--         ->  GroupAggregate  (cost=1045.51..1445.02 rows=4001 width=12) (actual time=7.505..27.492 rows=3463 loops=1)
-- "              Group Key: ""Product"".product_id"
--               ->  Merge Join  (cost=1045.51..1363.78 rows=8246 width=8) (actual time=7.475..26.206 rows=8230 loops=1)
-- "                    Merge Cond: (""Product"".product_id = ""Cart_product"".products_product_id)"
-- "                    ->  Index Only Scan using ""Product_pkey"" on ""Product""  (cost=0.28..164.29 rows=4001 width=4) (actual time=0.022..14.822 rows=4001 loops=1)"
--                           Heap Fetches: 1
--                     ->  Materialize  (cost=1045.22..1086.45 rows=8246 width=8) (actual time=7.443..9.844 rows=8230 loops=1)
--                           ->  Sort  (cost=1045.22..1065.84 rows=8246 width=8) (actual time=7.437..9.028 rows=8230 loops=1)
-- "                                Sort Key: ""Cart_product"".products_product_id"
--                                 Sort Method: external merge  Disk: 176kB
--                                 ->  Hash Join  (cost=110.70..393.34 rows=8246 width=8) (actual time=0.997..4.252 rows=8230 loops=1)
-- "                                      Hash Cond: (""Cart_product"".carts_cart_id = ""Carts"".cart_id)"
-- "                                      ->  Seq Scan on ""Cart_product""  (cost=0.00..158.95 rows=10995 width=8) (actual time=0.006..0.739 rows=10995 loops=1)"
--                                       ->  Hash  (cost=91.95..91.95 rows=1500 width=12) (actual time=0.983..0.985 rows=1500 loops=1)
--                                             Buckets: 2048  Batches: 1  Memory Usage: 81kB
--                                             ->  Hash Join  (cost=60.00..91.95 rows=1500 width=12) (actual time=0.383..0.795 rows=1500 loops=1)
-- "                                                  Hash Cond: (""Orders"".carts_cart_id = ""Carts"".cart_id)"
-- "                                                  ->  Seq Scan on ""Orders""  (cost=0.00..28.00 rows=1500 width=8) (actual time=0.005..0.141 rows=1500 loops=1)"
--                                                         Filter: (order_id IS NOT NULL)
--                                                   ->  Hash  (cost=35.00..35.00 rows=2000 width=4) (actual time=0.373..0.374 rows=2000 loops=1)
--                                                         Buckets: 2048  Batches: 1  Memory Usage: 87kB
-- "                                                        ->  Seq Scan on ""Carts""  (cost=0.00..35.00 rows=2000 width=4) (actual time=0.003..0.172 rows=2000 loops=1)"
-- Planning Time: 0.459 ms
-- Execution Time: 28.075 ms
--
-- CREATE INDEX ON "Product"(product_id);
-- Limit  (cost=1462.88..1462.90 rows=10 width=12) (actual time=13.514..13.522 rows=10 loops=1)
--   ->  Sort  (cost=1462.88..1472.88 rows=4001 width=12) (actual time=13.511..13.517 rows=10 loops=1)
-- "        Sort Key: (count(""Orders"".order_id)) DESC"
--         Sort Method: top-N heapsort  Memory: 25kB
--         ->  GroupAggregate  (cost=1045.51..1376.42 rows=4001 width=12) (actual time=8.271..13.006 rows=3463 loops=1)
-- "              Group Key: ""Product"".product_id"
--               ->  Merge Join  (cost=1045.51..1295.18 rows=8246 width=8) (actual time=8.246..11.558 rows=8230 loops=1)
-- "                    Merge Cond: (""Cart_product"".products_product_id = ""Product"".product_id)"
--                     ->  Sort  (cost=1045.22..1065.84 rows=8246 width=8) (actual time=8.213..9.271 rows=8230 loops=1)
-- "                          Sort Key: ""Cart_product"".products_product_id"
--                           Sort Method: external merge  Disk: 176kB
--                           ->  Hash Join  (cost=110.70..393.34 rows=8246 width=8) (actual time=1.094..4.133 rows=8230 loops=1)
-- "                                Hash Cond: (""Cart_product"".carts_cart_id = ""Carts"".cart_id)"
-- "                                ->  Seq Scan on ""Cart_product""  (cost=0.00..158.95 rows=10995 width=8) (actual time=0.011..0.808 rows=10995 loops=1)"
--                                 ->  Hash  (cost=91.95..91.95 rows=1500 width=12) (actual time=1.068..1.071 rows=1500 loops=1)
--                                       Buckets: 2048  Batches: 1  Memory Usage: 81kB
--                                       ->  Hash Join  (cost=60.00..91.95 rows=1500 width=12) (actual time=0.410..0.876 rows=1500 loops=1)
-- "                                            Hash Cond: (""Orders"".carts_cart_id = ""Carts"".cart_id)"
-- "                                            ->  Seq Scan on ""Orders""  (cost=0.00..28.00 rows=1500 width=8) (actual time=0.009..0.190 rows=1500 loops=1)"
--                                                   Filter: (order_id IS NOT NULL)
--                                             ->  Hash  (cost=35.00..35.00 rows=2000 width=4) (actual time=0.391..0.392 rows=2000 loops=1)
--                                                   Buckets: 2048  Batches: 1  Memory Usage: 87kB
-- "                                                  ->  Seq Scan on ""Carts""  (cost=0.00..35.00 rows=2000 width=4) (actual time=0.006..0.197 rows=2000 loops=1)"
-- "                    ->  Index Only Scan using ""Product_product_id_idx"" on ""Product""  (cost=0.28..116.29 rows=4001 width=4) (actual time=0.026..0.868 rows=4000 loops=1)"
--                           Heap Fetches: 0
-- Planning Time: 0.908 ms
-- Execution Time: 13.713 ms
--
-- SET work_mem TO '4MB';
-- Limit  (cost=749.02..749.04 rows=10 width=12) (actual time=14.658..14.676 rows=10 loops=1)
--   ->  Sort  (cost=749.02..759.02 rows=4001 width=12) (actual time=14.656..14.671 rows=10 loops=1)
-- "        Sort Key: (count(""Orders"".order_id)) DESC"
--         Sort Method: top-N heapsort  Memory: 25kB
--         ->  HashAggregate  (cost=622.55..662.56 rows=4001 width=12) (actual time=13.021..14.175 rows=3463 loops=1)
-- "              Group Key: ""Product"".product_id"
--               Batches: 1  Memory Usage: 721kB
--               ->  Hash Join  (cost=277.01..581.32 rows=8246 width=8) (actual time=2.163..9.651 rows=8230 loops=1)
-- "                    Hash Cond: (""Cart_product"".products_product_id = ""Product"".product_id)"
--                     ->  Hash Join  (cost=110.70..393.34 rows=8246 width=8) (actual time=1.073..5.067 rows=8230 loops=1)
-- "                          Hash Cond: (""Cart_product"".carts_cart_id = ""Carts"".cart_id)"
-- "                          ->  Seq Scan on ""Cart_product""  (cost=0.00..158.95 rows=10995 width=8) (actual time=0.006..0.892 rows=10995 loops=1)"
--                           ->  Hash  (cost=91.95..91.95 rows=1500 width=12) (actual time=1.060..1.068 rows=1500 loops=1)
--                                 Buckets: 2048  Batches: 1  Memory Usage: 81kB
--                                 ->  Hash Join  (cost=60.00..91.95 rows=1500 width=12) (actual time=0.409..0.869 rows=1500 loops=1)
-- "                                      Hash Cond: (""Orders"".carts_cart_id = ""Carts"".cart_id)"
-- "                                      ->  Seq Scan on ""Orders""  (cost=0.00..28.00 rows=1500 width=8) (actual time=0.005..0.170 rows=1500 loops=1)"
--                                             Filter: (order_id IS NOT NULL)
--                                       ->  Hash  (cost=35.00..35.00 rows=2000 width=4) (actual time=0.398..0.401 rows=2000 loops=1)
--                                             Buckets: 2048  Batches: 1  Memory Usage: 87kB
-- "                                            ->  Seq Scan on ""Carts""  (cost=0.00..35.00 rows=2000 width=4) (actual time=0.004..0.193 rows=2000 loops=1)"
--                     ->  Hash  (cost=116.29..116.29 rows=4001 width=4) (actual time=1.075..1.076 rows=4001 loops=1)
--                           Buckets: 4096  Batches: 1  Memory Usage: 173kB
-- "                          ->  Index Only Scan using ""Product_product_id_idx"" on ""Product""  (cost=0.28..116.29 rows=4001 width=4) (actual time=0.013..0.638 rows=4001 loops=1)"
--                                 Heap Fetches: 1
-- Planning Time: 0.449 ms
-- Execution Time: 14.812 ms
--
-- -------------------------------------
--
--
-- Default settings
-- Nested Loop Left Join  (cost=14412.96..23428.72 rows=92017 width=524) (actual time=86.286..219.464 rows=461266 loops=1)
-- "  Join Filter: (""Carts"".cart_id = ""Cart_product"".carts_cart_id)"
--   Rows Removed by Join Filter: 5
--   ->  Merge Left Join  (cost=14412.96..16526.38 rows=92017 width=12) (actual time=86.266..134.875 rows=92254 loops=1)
-- "        Merge Cond: (""Product"".product_id = ""Cart_product"".products_product_id)"
-- "        ->  Index Scan using ""Product_pkey"" on ""Product""  (cost=0.28..278.30 rows=3042 width=4) (actual time=0.006..1.854 rows=3046 loops=1)"
--               Filter: (price > '100'::double precision)
--               Rows Removed by Filter: 955
--         ->  Materialize  (cost=14412.62..15017.75 rows=121025 width=12) (actual time=86.255..115.507 rows=121025 loops=1)
--               ->  Sort  (cost=14412.62..14715.18 rows=121025 width=12) (actual time=86.252..101.804 rows=121025 loops=1)
-- "                    Sort Key: ""Cart_product"".products_product_id"
--                     Sort Method: external merge  Disk: 2616kB
--                     ->  Hash Left Join  (cost=60.00..2126.62 rows=121025 width=12) (actual time=0.392..37.106 rows=121025 loops=1)
-- "                          Hash Cond: (""Cart_product"".carts_cart_id = ""Carts"".cart_id)"
-- "                          ->  Seq Scan on ""Cart_product""  (cost=0.00..1748.25 rows=121025 width=8) (actual time=0.004..8.444 rows=121025 loops=1)"
--                           ->  Hash  (cost=35.00..35.00 rows=2000 width=4) (actual time=0.383..0.385 rows=2000 loops=1)
--                                 Buckets: 2048  Batches: 1  Memory Usage: 87kB
-- "                                ->  Seq Scan on ""Carts""  (cost=0.00..35.00 rows=2000 width=4) (actual time=0.005..0.185 rows=2000 loops=1)"
--   ->  Materialize  (cost=0.00..1.07 rows=5 width=516) (actual time=0.000..0.000 rows=5 loops=92254)
-- "        ->  Seq Scan on ""Order_status""  (cost=0.00..1.05 rows=5 width=516) (actual time=0.013..0.015 rows=5 loops=1)"
-- Planning Time: 0.317 ms
-- Execution Time: 235.495 ms
--
-- SET work_mem TO '15MB';
-- Nested Loop Left Join  (cost=12344.46..21057.66 rows=92017 width=524) (actual time=117.380..221.807 rows=461266 loops=1)
-- "  Join Filter: (""Carts"".cart_id = ""Cart_product"".carts_cart_id)"
--   Rows Removed by Join Filter: 5
--   ->  Merge Left Join  (cost=12344.46..14155.32 rows=92017 width=12) (actual time=117.355..153.034 rows=92254 loops=1)
-- "        Merge Cond: (""Product"".product_id = ""Cart_product"".products_product_id)"
-- "        ->  Index Scan using ""Product_pkey"" on ""Product""  (cost=0.28..278.30 rows=3042 width=4) (actual time=0.006..1.588 rows=3046 loops=1)"
--               Filter: (price > '100'::double precision)
--               Rows Removed by Filter: 955
--         ->  Sort  (cost=12344.12..12646.68 rows=121025 width=12) (actual time=117.338..136.309 rows=121025 loops=1)
-- "              Sort Key: ""Cart_product"".products_product_id"
--               Sort Method: quicksort  Memory: 8746kB
--               ->  Hash Left Join  (cost=60.00..2126.62 rows=121025 width=12) (actual time=0.475..62.736 rows=121025 loops=1)
-- "                    Hash Cond: (""Cart_product"".carts_cart_id = ""Carts"".cart_id)"
-- "                    ->  Seq Scan on ""Cart_product""  (cost=0.00..1748.25 rows=121025 width=8) (actual time=0.005..8.561 rows=121025 loops=1)"
--                     ->  Hash  (cost=35.00..35.00 rows=2000 width=4) (actual time=0.463..0.465 rows=2000 loops=1)
--                           Buckets: 2048  Batches: 1  Memory Usage: 87kB
-- "                          ->  Seq Scan on ""Carts""  (cost=0.00..35.00 rows=2000 width=4) (actual time=0.005..0.273 rows=2000 loops=1)"
--   ->  Materialize  (cost=0.00..1.07 rows=5 width=516) (actual time=0.000..0.000 rows=5 loops=92254)
-- "        ->  Seq Scan on ""Order_status""  (cost=0.00..1.05 rows=5 width=516) (actual time=0.017..0.018 rows=5 loops=1)"
-- Planning Time: 0.273 ms
-- Execution Time: 234.946 ms
--
-- CREATE INDEX ON "Cart_product"(products_product_id);
-- Nested Loop Left Join  (cost=0.86..15721.21 rows=92017 width=524) (actual time=0.053..238.353 rows=461266 loops=1)
-- "  Join Filter: (""Carts"".cart_id = ""Cart_product"".carts_cart_id)"
--   Rows Removed by Join Filter: 5
--   ->  Nested Loop Left Join  (cost=0.86..8818.88 rows=92017 width=12) (actual time=0.039..150.460 rows=92254 loops=1)
--         ->  Merge Left Join  (cost=0.57..5928.15 rows=92017 width=8) (actual time=0.025..85.262 rows=92254 loops=1)
-- "              Merge Cond: (""Product"".product_id = ""Cart_product"".products_product_id)"
-- "              ->  Index Scan using ""Product_pkey"" on ""Product""  (cost=0.28..278.30 rows=3042 width=4) (actual time=0.011..1.388 rows=3046 loops=1)"
--                     Filter: (price > '100'::double precision)
--                     Rows Removed by Filter: 955
-- "              ->  Index Scan using ""Cart_product_products_product_id_idx"" on ""Cart_product""  (cost=0.29..4419.51 rows=121025 width=8) (actual time=0.010..68.443 rows=121025 loops=1)"
--         ->  Memoize  (cost=0.29..0.31 rows=1 width=4) (actual time=0.000..0.000 rows=1 loops=92254)
-- "              Cache Key: ""Cart_product"".carts_cart_id"
--               Cache Mode: logical
--               Hits: 90253  Misses: 2001  Evictions: 0  Overflows: 0  Memory Usage: 204kB
-- "              ->  Index Only Scan using ""Carts_pkey"" on ""Carts""  (cost=0.28..0.30 rows=1 width=4) (actual time=0.011..0.011 rows=1 loops=2001)"
-- "                    Index Cond: (cart_id = ""Cart_product"".carts_cart_id)"
--                     Heap Fetches: 369
--   ->  Materialize  (cost=0.00..1.07 rows=5 width=516) (actual time=0.000..0.000 rows=5 loops=92254)
-- "        ->  Seq Scan on ""Order_status""  (cost=0.00..1.05 rows=5 width=516) (actual time=0.009..0.010 rows=5 loops=1)"
-- Planning Time: 1.083 ms
-- Execution Time: 253.319 ms
--
--
-- -------------------------------------
--
-- Default settings
-- Limit  (cost=240.46..240.47 rows=1 width=68) (actual time=1.398..1.402 rows=5 loops=1)
--   ->  Sort  (cost=240.46..240.47 rows=1 width=68) (actual time=1.397..1.399 rows=5 loops=1)
-- "        Sort Key: (count(""Orders"".order_id)) DESC"
--         Sort Method: top-N heapsort  Memory: 25kB
--         ->  GroupAggregate  (cost=240.43..240.45 rows=1 width=68) (actual time=1.380..1.391 rows=20 loops=1)
-- "              Group Key: ""Users"".user_id"
--               ->  Sort  (cost=240.43..240.44 rows=1 width=60) (actual time=1.374..1.377 rows=20 loops=1)
-- "                    Sort Key: ""Users"".user_id"
--                     Sort Method: quicksort  Memory: 27kB
--                     ->  Hash Right Join  (cost=206.79..240.42 rows=1 width=60) (actual time=1.219..1.368 rows=20 loops=1)
-- "                          Hash Cond: (""Orders"".carts_cart_id = ""Carts"".cart_id)"
-- "                          ->  Seq Scan on ""Orders""  (cost=0.00..28.00 rows=1500 width=8) (actual time=0.004..0.093 rows=1500 loops=1)"
--                           ->  Hash  (cost=206.77..206.77 rows=1 width=56) (actual time=1.145..1.147 rows=20 loops=1)
--                                 Buckets: 1024  Batches: 1  Memory Usage: 10kB
--                                 ->  Hash Right Join  (cost=166.51..206.77 rows=1 width=56) (actual time=0.946..1.140 rows=20 loops=1)
-- "                                      Hash Cond: (""Carts"".users_user_id = ""Users"".user_id)"
-- "                                      ->  Seq Scan on ""Carts""  (cost=0.00..35.00 rows=2000 width=8) (actual time=0.013..0.113 rows=2000 loops=1)"
--                                       ->  Hash  (cost=166.50..166.50 rows=1 width=52) (actual time=0.879..0.880 rows=20 loops=1)
--                                             Buckets: 1024  Batches: 1  Memory Usage: 10kB
-- "                                            ->  Seq Scan on ""Users""  (cost=0.00..166.50 rows=1 width=52) (actual time=0.112..0.868 rows=20 loops=1)"
--                                                   Filter: ((phone_number)::text ~~ '%adb%'::text)
--                                                   Rows Removed by Filter: 2980
-- Planning Time: 0.224 ms
-- Execution Time: 1.435 ms
--
-- CREATE INDEX trgm_idx_user_phone ON "Users" USING  gin (phone_number gin_trgm_ops);
-- Limit  (cost=89.98..89.98 rows=1 width=68) (actual time=0.776..0.780 rows=5 loops=1)
--   ->  Sort  (cost=89.98..89.98 rows=1 width=68) (actual time=0.775..0.778 rows=5 loops=1)
-- "        Sort Key: (count(""Orders"".order_id)) DESC"
--         Sort Method: top-N heapsort  Memory: 25kB
--         ->  GroupAggregate  (cost=89.95..89.97 rows=1 width=68) (actual time=0.754..0.768 rows=20 loops=1)
-- "              Group Key: ""Users"".user_id"
--               ->  Sort  (cost=89.95..89.95 rows=1 width=60) (actual time=0.747..0.751 rows=20 loops=1)
-- "                    Sort Key: ""Users"".user_id"
--                     Sort Method: quicksort  Memory: 27kB
--                     ->  Hash Right Join  (cost=56.30..89.94 rows=1 width=60) (actual time=0.543..0.740 rows=20 loops=1)
-- "                          Hash Cond: (""Orders"".carts_cart_id = ""Carts"".cart_id)"
-- "                          ->  Seq Scan on ""Orders""  (cost=0.00..28.00 rows=1500 width=8) (actual time=0.006..0.117 rows=1500 loops=1)"
--                           ->  Hash  (cost=56.29..56.29 rows=1 width=56) (actual time=0.464..0.466 rows=20 loops=1)
--                                 Buckets: 1024  Batches: 1  Memory Usage: 10kB
--                                 ->  Hash Right Join  (cost=16.03..56.29 rows=1 width=56) (actual time=0.172..0.459 rows=20 loops=1)
-- "                                      Hash Cond: (""Carts"".users_user_id = ""Users"".user_id)"
-- "                                      ->  Seq Scan on ""Carts""  (cost=0.00..35.00 rows=2000 width=8) (actual time=0.005..0.192 rows=2000 loops=1)"
--                                       ->  Hash  (cost=16.02..16.02 rows=1 width=52) (actual time=0.054..0.055 rows=20 loops=1)
--                                             Buckets: 1024  Batches: 1  Memory Usage: 10kB
-- "                                            ->  Bitmap Heap Scan on ""Users""  (cost=12.00..16.02 rows=1 width=52) (actual time=0.023..0.048 rows=20 loops=1)"
--                                                   Recheck Cond: ((phone_number)::text ~~ '%adb%'::text)
--                                                   Heap Blocks: exact=17
--                                                   ->  Bitmap Index Scan on trgm_idx_user_phone  (cost=0.00..12.00 rows=1 width=0) (actual time=0.015..0.015 rows=20 loops=1)
--                                                         Index Cond: ((phone_number)::text ~~ '%adb%'::text)
-- Planning Time: 0.554 ms
-- Execution Time: 0.835 ms