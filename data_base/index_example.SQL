CREATE TABLE perf_test(
    id int,
    reason text COLLATE "C",
    annotation text COLLATE "C"
);

INSERT INTO perf_test(id, reason, annotation)
SELECT s.id, md5(random()::text), NULL
from generate_series(1, 100000) AS s(id)
ORDER BY random();

UPDATE perf_test
SET annotation = md5(UPPER(random()::text))
WHERE annotation is NULL;

EXPLAIN
SELECT *
from perf_test
where id = 999999;

CREATE INDEX idx_perf_test ON perf_test(id);

CREATE INDEX idx_perf_test_annotation ON perf_test(reason, annotation);


EXPLAIN
SELECT *
FROM perf_test
WHERE reason LIKE 'bc%' AND annotation LIKE 'ab%';

ANALYSE perf_test;

EXPLAIN
SELECT *
FROM perf_test
where reason like 'bc%'

EXPLAIN
SELECT *
FROM perf_test
where perf_test.annotation like 'bc%';

CREATE INDEX idx_perf_test_annot ON perf_test(annotation);

explain
SELECT *
FROM perf_test
WHERE LOWER(annotation) LIKE ('ab%');

CREATE INDEX idx_perf_test_annotation_lower ON perf_test(LOWER(annotation));

EXPLAIN
SELECT *
FROM perf_test
WHERE reason LIKE '%bce%';

CREATE EXTENSION pg_trgm;
CREATE INDEX trgm_idx_perf_tesr_reason ON perf_test USING  gin (reason gin_trgm_ops);