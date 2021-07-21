create procedure init()
    language plpgsql as
$$
begin
    drop table if exists bids;
    drop table if exists result;
    create table bids
    (
        price  double precision,
        time   double precision,
        volume double precision
    );

    create table result
    (
        price  double precision,
        time   double precision,
        volume double precision,
        agg    double precision
    );

end;
$$;

create function queryNaive() returns integer
    language plpgsql as
$$
declare
    StartTime timestamptz;
    EndTime   timestamptz;
    Delta     double precision;
begin
    StartTime := clock_timestamp();
    insert into result
    select b1.price,
           b1.time,
           b1.volume,
           sum(1.0)
    from bids b1
             join bids b2 on b2.time = b1.time and b2.price < b1.price
    group by b1.price, b1.time, b1.volume;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;

create function querySmart() returns integer
    language plpgsql as
$$
declare
    StartTime timestamptz;
    EndTime   timestamptz;
    Delta     double precision;
begin
    StartTime := clock_timestamp();
    create temp table aggbids on commit drop as
    select time, price, sum(1.0) as agg
    from bids
    group by time, price;

    create temp table distbids on commit drop as
    select price, time, volume, sum(1) as agg
    from bids
    group by price, time, volume;

    create temp table cumaggbids on commit drop as
    select b1.time, b1.price, sum(b2.agg) as agg
    from aggbids b1
             join aggbids b2 on b2.time = b1.time and b2.price < b1.price
    group by b1.time, b1.price;

    insert into result
    select b1.price, b1.time, b1.volume, b1.agg * b2.agg
    from distbids b1
             join cumaggbids b2 on b2.time = b1.time and b2.price = b1.price;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;


create function queryRange() returns integer
    language plpgsql as
$$
declare
    StartTime timestamptz;
    EndTime   timestamptz;
    Delta     double precision;
    lp        integer;
begin
    StartTime := clock_timestamp();

    select log(2, count(distinct price))::integer
    into lp
    from bids;

    create temp table aggbids on commit drop as
    select time, price, sum(1.0) as agg
    from bids
    group by time, price;

    create temp table distbids on commit drop as
    select price, time, volume, sum(1) as agg
    from bids
    group by price, time, volume;


    call construct_rt_b2(lp);

    create temp table cumaggbids on commit drop as
    select b1.time, b1.price, (f).aggb2 as agg
    from aggbids b1,
         lateral (select lookup_rt_b2(b1.*, lp) as f offset 0) func;

    insert into result
    select b1.price, b1.time, b1.volume, b1.agg * b2.agg
    from distbids b1
             join cumaggbids b2 on b2.time = b1.time and b2.price = b1.price;

    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;


create function queryMerge() returns integer
    language plpgsql as
$$
declare
    StartTime timestamptz;
    EndTime   timestamptz;
    Delta     double precision;
    curb2 cursor for select *
                     from cube_b2;
begin
    StartTime := clock_timestamp();
    create temp table aggbids on commit drop as
    select time, price, sum(1.0) as agg
    from bids
    group by time, price
    order by time asc, price asc;

    create temp table distbids on commit drop as
    select price, time, volume, sum(1) as agg
    from bids
    group by price, time, volume
    order by time asc, price asc;

    call construct_cube_b2();

    open curb2;
    move next from curb2;
    insert into result
    select b1.price, b1.time, b1.volume, (f).aggb2 * b1.agg
    from distbids b1,
         lateral (select lookup_cube_b2(b1.*, curb2) as f offset 0) func;
    close curb2;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;