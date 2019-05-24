
-- array to named columns

CREATE OR REPLACE FUNCTION ch09eu.arr_to_input_16_users(arr ARRAY<FLOAT64>)
RETURNS 
STRUCT<u1 FLOAT64, u2 FLOAT64, u3 FLOAT64, u4 FLOAT64,
       u5 FLOAT64, u6 FLOAT64, u7 FLOAT64, u8 FLOAT64, 
       u9 FLOAT64, u10 FLOAT64, u11 FLOAT64, u12 FLOAT64, 
       u13 FLOAT64, u14 FLOAT64, u15 FLOAT64, u16 FLOAT64>

AS (
STRUCT(
    arr[OFFSET(0)]
    , arr[OFFSET(1)]
    , arr[OFFSET(2)]
    , arr[OFFSET(3)]
    , arr[OFFSET(4)]
    , arr[OFFSET(5)]
    , arr[OFFSET(6)]
    , arr[OFFSET(7)]
    , arr[OFFSET(8)]
    , arr[OFFSET(9)]
    , arr[OFFSET(10)]
    , arr[OFFSET(11)]
    , arr[OFFSET(12)]
    , arr[OFFSET(13)]
    , arr[OFFSET(14)]
    , arr[OFFSET(15)]
));

CREATE OR REPLACE FUNCTION ch09eu.arr_to_input_16_products(arr ARRAY<FLOAT64>)
RETURNS 
STRUCT<p1 FLOAT64, p2 FLOAT64, p3 FLOAT64, p4 FLOAT64,
       p5 FLOAT64, p6 FLOAT64, p7 FLOAT64, p8 FLOAT64, 
       p9 FLOAT64, p10 FLOAT64, p11 FLOAT64, p12 FLOAT64, 
       p13 FLOAT64, p14 FLOAT64, p15 FLOAT64, p16 FLOAT64>

AS (
STRUCT(
    arr[OFFSET(0)]
    , arr[OFFSET(1)]
    , arr[OFFSET(2)]
    , arr[OFFSET(3)]
    , arr[OFFSET(4)]
    , arr[OFFSET(5)]
    , arr[OFFSET(6)]
    , arr[OFFSET(7)]
    , arr[OFFSET(8)]
    , arr[OFFSET(9)]
    , arr[OFFSET(10)]
    , arr[OFFSET(11)]
    , arr[OFFSET(12)]
    , arr[OFFSET(13)]
    , arr[OFFSET(14)]
    , arr[OFFSET(15)]
));

