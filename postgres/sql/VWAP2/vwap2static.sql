create table bids_temp
(
    price  double precision,
    time   double precision,
    volume double precision
);

alter table bids_temp
    owner to postgres;

create table vwap2res
(
    time double precision,
    vwap double precision
);

alter table vwap2res
    owner to postgres;


create table b1b3dist
(
    time  double precision,
    aggb3 double precision
);

create table b1b3
(
    time   double precision,
    price  double precision,
    volume double precision,
    aggb3  double precision
);

alter table b1b3
    owner to postgres;

create table b1b3b2
(
    time   double precision,
    price  double precision,
    volume double precision,
    aggb3  double precision,
    aggb2  double precision
);

alter table b1b3b2
    owner to postgres;

create procedure querynaive()
    language plpgsql
as
$$
declare

begin

    delete from b1b3b2;
    insert into b1b3b2
    select b1.time,
           b1.price,
           b1.volume,
           (select sum(0.25 * b3.volume) from bids b3 where b3.time <= b1.time),
           (select sum(b2.volume) from bids b2 where b2.price < b1.price and b2.time <= b1.time)
    from bids b1;

    delete from vwap2res;
    insert into vwap2res
    select time, sum(price * volume)
    from b1b3b2
    where aggb3 < aggb2
    group by time;
end;
$$;

alter procedure querynaive() owner to postgres;

create procedure querymerge()
    language plpgsql
as
$$
declare
    _curb3 cursor for select *
                      from cube_b3;
    curb2 cursor for select *
                     from cube_b2;

begin

    call construct_cube_b2();
    call construct_cube_b3();

    create or replace view aggbids3 as
    select distinct time from bids order by time;

    open _curb3;
    move next from _curb3;

    delete from b1b3dist;
    insert into b1b3dist
    select b1.time, (f).*
    from aggbids3 b1,
         lateral (select lookup_cube_b3(b1.*, _curb3) as f offset 0) func
    order by time;
    close _curb3;

    delete from b1b3;
    insert into b1b3
    select b1.time, b1.price, b1.volume, t.aggb3
    from (select * from bids order by time, price) b1
             join
         b1b3dist t on b1.time = t.time
    order by time, price;


    open curb2;
    move next from curb2;

    delete from b1b3b2;
    insert into b1b3b2
    select time, price, volume, aggb3, (f).*
    from b1b3,
         lateral (select lookup_cube_b2(b1b3.*, curb2) as f offset 0) func;

    close curb2;

    delete from vwap2res;
    insert into vwap2res
    select time, sum(price * volume)
    from b1b3b2
    where aggb3 < aggb2
    group by time;

end;
$$;

alter procedure querymerge() owner to postgres;

create procedure queryrange(lb3 integer, ltb2 integer, lpb2 integer)
    language plpgsql
as
$$
declare
    bfb3      integer          := 2;
    bfb2      integer          := 2;
    StartTime timestamptz;
    EndTime   timestamptz;
    Delta     double precision;
    Delta1    double precision := 0;
    Delta2    double precision := 0;
    Delta3    double precision := 0;
    Delta4    double precision := 0;

begin
    StartTime := clock_timestamp();
    call construct_rt_b3(lb3, bfb3);
    call construct_rt_b2(ltb2, bfb2, lpb2, bfb2);
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    Delta1 := Delta1 + Delta;

    StartTime := clock_timestamp();
    delete from b1b3;
    insert into b1b3
    select b1.time, b1.price, b1.volume, (f).*
    from bids b1,
         lateral (select lookup_rt_b3(b1.*, lb3) as f offset 0) func;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    Delta2 := Delta2 + Delta;

    StartTime := clock_timestamp();
    delete from b1b3b2;
    insert into b1b3b2
    select time, price, volume, aggb3, (f).*
    from b1b3,
         lateral (select lookup_rt_b2(b1b3.*, ltb2, lpb2) as f offset 0) func;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    Delta3 := Delta3 + Delta;

    StartTime := clock_timestamp();
    delete from vwap2res;
    insert into vwap2res
    select time, sum(price * volume)
    from b1b3b2
    where aggb3 < aggb2
    group by time;
    EndTime := clock_timestamp();
    Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
    Delta4 := Delta4 + Delta;

    raise notice 'RT construct = %,  lookupb3 = %, lookupb2 = %, final = %', Delta1, Delta2, Delta3, Delta4;
end;

$$;

alter procedure queryrange(integer, integer, integer) owner to postgres;

