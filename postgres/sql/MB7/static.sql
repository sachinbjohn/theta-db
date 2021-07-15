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
    b2time double precision,
    agg    double precision
);

create procedure iniNaive()
    language plpgsql as
$$
begin
    delete from bids;
    delete from result;
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
    select *
    from bids b1,
         lateral (select b2.time, sum(1) from bids b2 where b2.price < b1.price group by b2.time) q;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;

create procedure initSmart()
    language plpgsql as
$$
begin
    delete from bids;
    delete from result;
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

    create or replace view aggbids as
    select time, price, sum(1) as agg
    from bids
    group by time, price;

    create or replace view cumaggbids as
    select b1.price, b2.time, sum(b2.agg) as agg
    from aggbids b1
             join aggbids b2 on b2.price < b1.price
    group by b1.price, b2.time;

    insert into result
    select b1.price, b1.time, b1.volume, c.time, c.agg
    from bids b1
             join cumaggbids c on b1.price = c.price;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;

create procedure initRange()
    language plpgsql as
$$
begin
    delete from bids;
    delete from result;
    delete from rt_b2;
    delete from rt_b2_new;
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

    create or replace view aggbids as
    select time, price, sum(1) as agg
    from bids
    group by time, price;

    call construct_rt_b2(lp);
    insert into result
    select b1.*, (f).*
    from bids b1,
         lateral (select lookup_rt_b2(b1.*, lp) as f offset 0) func;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;

create procedure initMerge()
    language plpgsql as
$$
begin
    delete from bids;
    delete from result;
    delete from cube_b2;
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

    create or replace view aggbids as
    select time, price, sum(1) as agg
    from bids
    group by time, price
    order by price asc, time asc;

    call construct_cube_b2();

    open curb2;
    move next from curb2;
    insert into result
    select b1.*, (f).*
    from (select * from bids order by price asc, time asc) b1,
         lateral (select lookup_cube_b2(b1.*, curb2) as f offset 0) func;
    close curb2;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;