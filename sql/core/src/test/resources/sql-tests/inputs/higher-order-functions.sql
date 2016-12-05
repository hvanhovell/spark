-- Only allow lambda's in higher order functions.
select upper(x -> x) as v;

-- Identity transform an array
select transform(array(1,2,3,4,5), x -> x) as v;

-- Transform an array
select transform(array(1,2,3,4,5), x -> x * x) as v;

-- Check for element existence
select exists(array(1,2,3,4,5), x -> x = 2) as v;

-- Filer.
select filter(array(1,2,3,4,5), x -> x > 2) as v;

-- Reduce.
select reduce(array(1,2,3,4,5), 0, (x, a) -> x + a) as v;

-- Do not allow ambiguous lambda arguments.
select reduce(array(1,2,3,4,5), 0, (x, X) -> x) as v;

-- Numer of arguments should match the expected number of arguments.
select reduce(array(1,2,3,4,5), 0, x -> x) as v;

-- Filter nested arrays
select transform(array(array(1,2), array(2,3)), x -> filter(x, y -> y > 2)) as v;

-- Reduce nested arrays
select transform(array(array(1,2), array(2,3)), x -> reduce(x, 1, (y, z) -> y * z)) as v;
