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
        agg double precision
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
    lp integer;
    lt integer;
begin
    StartTime := clock_timestamp();

    select log(2, count(distinct time))::integer
    into lt
    from bidsS;

    select log(2, count(distinct price))::integer
    into lp
    from bidsS;

    create temp table aggbidsR on commit drop as
    select time, price, sum(1.0) as agg
    from bidsR
    group by time, price;


    create temp table aggbidsS on commit drop as
    select time, price, sum(1.0) as agg
    from bidsS
    group by time, price;

    call construct_rt_bS(lt, lp);


    insert into result
    select bR.agg * (f).aggbS
    from aggbidsR bR,
         lateral (select lookup_rt_bS(bR.*, lt, lp) as f offset 0) func;

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
    lt integer;
    lp integer;
begin
    StartTime := clock_timestamp();

    select log(2, count(distinct time))::integer
    into lt
    from bidsR;

    select log(2, count(distinct price))::integer
    into lp
    from bidsR;

    create temp table aggbidsR on commit drop as
    select time, price, sum(1.0) as agg
    from bidsR
    group by time, price;


    create temp table aggbidsS on commit drop as
    select time, price, sum(1.0) as agg
    from bidsS
    group by time, price;

    call construct_rt_bR(lt, lp);


    insert into result
    select bS.agg * (f).aggbR
    from aggbidsS bS,
         lateral (select lookup_rt_bR(bS.*, lt, lp) as f offset 0) func;

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
    select time, price, sum(1.0) as agg
    from bidsR
    group by time, price
    order by time asc, price asc;

    create temp table aggbidsS on commit drop as
    select time, price, sum(1.0) as agg
    from bidsS
    group by time, price
    order by time asc, price asc;


    call construct_cube_bS();

    open curbS;
    move next from curbS;
    insert into result
    select bR.agg * (f).aggbS
    from aggbidsR bR,
         lateral (select lookup_cube_bS(bR.*, curbS) as f offset 0) func;
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
    select time, price, sum(1.0) as agg
    from bidsR
    group by time, price
    order by time desc, price desc;

    create temp table aggbidsS on commit drop as
    select time, price, sum(1.0) as agg
    from bidsS
    group by time, price
    order by time desc, price desc;


    call construct_cube_bR();

    open curbR;
    move next from curbR;
    insert into result
    select bS.agg * (f).aggbR
    from aggbidsS bS,
         lateral (select lookup_cube_bR(bS.*, curbR) as f offset 0) func;
    close curbR;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    return Delta::integer;
end;
$$;