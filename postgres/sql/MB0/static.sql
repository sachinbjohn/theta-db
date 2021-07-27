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
           sum(1)
    from bids b1,
         bids b2
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
    agg       double precision := 0;
begin
    StartTime := clock_timestamp();
    select sum(1) into agg from bids;

    insert into result
    select b1.price, b1.time, b1.volume, sum(1) * agg
    from bids b1
    group by b1.price, b1.time, b1.volume;

    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;