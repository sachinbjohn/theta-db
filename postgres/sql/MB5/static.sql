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

create function queryNaive(lp integer, lt integer) returns integer
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
           (select sum(1.0) from bids b2 where b2.time < b1.time and b2.time > b1.time - 5)
    from bids b1;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;


create function querySmart(lp integer, lt integer) returns integer
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

    create temp table cumaggbids on commit drop as
    select b1.time, sum(b2.agg) as agg
    from aggbids b1
             left join aggbids b2 on b2.time < b1.time and b2.time > b1.time - 5
    group by b1.time;

    insert
    into result
    select b1.price, b1.time, b1.volume, agg
    from bids b1
             join cumaggbids c on b1.time = c.time;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;


create function queryRange(lp integer, lt integer) returns integer
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

    call construct_rt_b2(lt, lt);

    insert into result
    select b1.*, (f).*
    from bids b1,
         lateral (select lookup_rt_b2(b1.*, lt, lt) as f offset 0) func;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;


create function queryMerge(lp integer, lt integer) returns integer
    language plpgsql as
$$
declare
    curb2 cursor for select *
                     from cube_b2;
    StartTime timestamptz;
    EndTime   timestamptz;
    Delta     double precision;
begin
    StartTime := clock_timestamp();

    create temp table aggbids on commit drop as
    select time, sum(1.0) as agg
    from bids
    group by time
    order by time;

    call construct_cube_b2();

    open curb2;
    move next from curb2;
    insert into result
    select b1.*, (f).*
    from (select * from bids order by time asc, time - 5 desc) b1,
         lateral (select lookup_cube_b2(b1.*, curb2) as f offset 0) func;
    close curb2;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;