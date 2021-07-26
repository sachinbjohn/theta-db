create procedure init()
    language plpgsql as
$$
begin
    drop table if exists bidsR;
    drop table if exists bidsS;
    drop table if exists result;
    create table bidsR
    (
        price  double precision,
        time   double precision,
        volume double precision
    );
    create table bidsS
    (
        like bidsR
    );
    create table result
    (
        time double precision,
        agg  double precision
    );

end;
$$;


create function queryRSRange() returns integer
    language plpgsql as
$$
declare
    StartTime timestamptz;
    EndTime   timestamptz;
    Delta     double precision;
begin
    StartTime := clock_timestamp();

    create temp table aggbidsR on commit drop as
    select price, sum(1.0) as agg
    from bidsR
    group by price;


    create temp table aggbidsS on commit drop as
    select time, price, sum(1.0) as agg
    from bidsS
    group by time, price;

    call construct_rt_bS();


    insert into result
    select (f).gbykey1, sum(bR.agg * (f).aggbS)
    from aggbidsR bR,
         lateral (select lookup_rt_bS(bR.*) as f offset 0) func
    group by (f).gbykey1;

    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;

create function querySRRange() returns integer
    language plpgsql as
$$
declare
    StartTime timestamptz;
    EndTime   timestamptz;
    Delta     double precision;
begin
    StartTime := clock_timestamp();

    create temp table aggbidsR on commit drop as
    select price, sum(1.0) as agg
    from bidsR
    group by price;


    create temp table aggbidsS on commit drop as
    select time, price, sum(1.0) as agg
    from bidsS
    group by time, price;

    create temp table distBids on commit drop as
    select distinct price from bidsS;

    call construct_rt_bR();


    create temp table cumaggbids on commit drop as
    select bS.price, (f).aggbR as agg
    from distBids bS,
         lateral (select lookup_rt_bR(bS.*) as f offset 0) func;

    insert into result
    select bS.time, sum(bR.agg * bS.agg)
    from aggbidsS bS
             join cumaggbids bR on bS.price = bR.price
    group by bS.time;

    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;


create function queryRSMerge() returns integer
    language plpgsql as
$$
declare
    curbS cursor for select *
                     from cube_bS;
    StartTime timestamptz;
    EndTime   timestamptz;
    Delta     double precision;
begin
    StartTime := clock_timestamp();

    create temp table aggbidsR on commit drop as
    select price, sum(1.0) as agg
    from bidsR
    group by price
    order by price asc;

    create temp table aggbidsS on commit drop as
    select time, price, sum(1.0) as agg
    from bidsS
    group by time, price
    order by time asc, price asc;


    call construct_cube_bS();

    open curbS;
    move next from curbS;
    insert into result
    select (f).gbykey1, sum(bR.agg * (f).aggbS)
    from aggbidsR bR,
         lateral (select lookup_cube_bS(bR.*, curbS) as f offset 0) func
    group by (f).gbykey1;
    close curbS;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;

create function querySRMerge() returns integer
    language plpgsql as
$$
declare
    curbR cursor for select *
                     from cube_bR;
    StartTime timestamptz;
    EndTime   timestamptz;
    Delta     double precision;
begin
    StartTime := clock_timestamp();

    create temp table aggbidsR on commit drop as
    select price, sum(1.0) as agg
    from bidsR
    group by price
    order by price desc;

    create temp table aggbidsS on commit drop as
    select time, price, sum(1.0) as agg
    from bidsS
    group by time, price
    order by price desc;


    call construct_cube_bR();

    open curbR;
    move next from curbR;
    insert into result
    select bS.time, sum(bS.agg * (f).aggbR)
    from aggbidsS bS,
         lateral (select lookup_cube_bR(bS.*, curbR) as f offset 0) func
    group by bS.time;

    close curbR;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;