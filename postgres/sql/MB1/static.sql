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
create table result
(
    price  double precision,
    time   double precision,
    volume double precision,
    agg    double precision
);


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
           sum(1)
    from bids b1
             join bids b2 on b2.time = b1.time
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
    select time, sum(1.0) as agg
    from bids
    group by time;

    create temp table distbids on commit drop as
    select price, time, volume, sum(1) as agg
    from bids
    group by price, time, volume;

    insert into result
    select b1.price, b1.time, b1.volume, b1.agg * b2.agg
    from distbids b1
             join aggbids b2 on b2.time = b1.time;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;
