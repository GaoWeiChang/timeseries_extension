CREATE TABLE sensor_data (
    time        TIMESTAMPTZ NOT NULL,
    sensor_id   INTEGER,
    temperature DOUBLE PRECISION,
    count       INTEGER
);

INSERT INTO sensor_data
SELECT
    '2024-01-01'::timestamptz + (i || ' minutes')::interval,
    i,                                  -- 1, 2, 3, 4, ...  (delta = +1)
    20 + (random() * 10),
    i * 10                              -- 10, 20, 30, 40, ... (delta = +10)
FROM generate_series(1, 20) i;

-- delta + 1
SELECT test_compress_delta('sensor_data', 'sensor_id');

-- delta + 10
SELECT test_compress_delta('sensor_data', 'count');



DROP TABLE IF EXISTS irregular_data CASCADE;

CREATE TABLE irregular_data (
    id    SERIAL,
    value INTEGER
);

INSERT INTO irregular_data (value) VALUES
    (100),   -- base
    (103),   -- +3
    (105),   -- +2
    (102),   -- -3
    (110),   -- +8
    (95),    -- -15
    (95),    -- 0
    (200);   -- +105

SELECT test_compress_delta('irregular_data', 'value');


DROP TABLE IF EXISTS negative_data CASCADE;

CREATE TABLE negative_data (
    id    SERIAL,
    value INTEGER
);


-- negative data
INSERT INTO negative_data (value) VALUES
    (-50),
    (-45),    -- +5
    (-40),    -- +5
    (-60),    -- -20
    (-10);    -- +50

SELECT test_compress_delta('negative_data', 'value');

-- large number
CREATE TABLE bigint_data (
    id    SERIAL,
    value BIGINT
);

INSERT INTO bigint_data (value) VALUES
    (1000000000),
    (1000000010),   -- +10
    (1000000025),   -- +15
    (1000000020),   -- -5
    (1000000100);   -- +80

SELECT test_compress_delta('bigint_data', 'value');