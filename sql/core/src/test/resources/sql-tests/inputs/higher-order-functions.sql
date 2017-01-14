create or replace temporary view nested as values
  (1, array(32, 97), array(array(12, 99), array(123, 42), array(1))),
  (2, array(77, -76), array(array(6, 96, 65), array(-1, -2))),
  (3, array(12), array(array(17)))
  as t(x, ys, zs);

-- Only allow lambda's in higher order functions.
select upper(x -> x) as v;

-- Identity transform an array
select transform(zs, z -> z) as v from nested;

-- Transform an array
select transform(ys, y -> y * y) as v from nested;

-- Check for element existence
select exists(ys, y -> y > 30) as v from nested;

-- Filer.
select filter(ys, y -> y > 30) as v from nested;

-- Reduce.
select reduce(ys, 0, (y, a) -> y + a + x) as v from nested;

-- Do not allow ambiguous lambda arguments.
select reduce(ys, 0, (y, Y) -> y) as v from nested;

-- Number of arguments should match the expected number of arguments.
select reduce(ys, 0, y -> y) as v from nested;
select reduce(ys, 0, (a, b, c) -> a + b + c) as v from nested;

-- Filter nested arrays
select transform(zs, z -> filter(z, zz -> zz > 50)) as v from nested;

-- Reduce nested arrays
select transform(zs, z -> reduce(z, 1, (acc, val) -> acc * val * size(z))) as v from nested;
