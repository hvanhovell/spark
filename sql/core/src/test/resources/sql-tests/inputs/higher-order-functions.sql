select transform(array(1,2,3,4,5), x -> x * x) as v;

select exists(array(1,2,3,4,5), x -> x = 2) as v;

select filter(array(1,2,3,4,5), x -> x > 2) as v;

select reduce(array(1,2,3,4,5), 0, (x, a) -> x + a) as v;
