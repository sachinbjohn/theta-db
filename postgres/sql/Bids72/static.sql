create table bids_temp
(
    price  double precision,
    time   double precision,
    volume double precision
);

create table b1resdist
(
    time   double precision,
    agg    double precision
);

create table b1res
(
    price  double precision,
    time   double precision,
    volume double precision,
    agg    double precision
);

create table b32dist
(
    time   double precision,
    price  double precision,
    b2time double precision,
    b2agg  double precision
);

create table b32
(
    price  double precision,
    time   double precision,
    volume double precision,
    b2time double precision,
    b2agg  double precision
);
create procedure querynaive()
    language plpgsql as
$$
begin
    delete from b1res;

    insert into b1res
    select b1.price,
           b1.time,
           b1.volume,
           (select sum(1)
            from bids b2,
                 bids b3
            where b3.price > b2.price
              and b3.time < b2.time
              and b2.time < b1.time)
    from bids b1;
end;
$$;

create procedure queryrange(lp integer, lt integer)
    language plpgsql as
$$
begin
    call construct_rt_b2(lt, lp);

    delete from b32;
    insert into b32
    select b3.*, (f).*
    from bids b3,
         lateral (select lookup_rt_b2(b3.*, lt, lp) as f) func;

    call construct_rt_b32(lt);

    delete from b1res;
    insert into b1res
    select b1.*, (f).*
    from bids b1,
         lateral (select lookup_rt_b32(b1.*, lt) as f) func;
end;
$$;

create procedure querymerge()
    language plpgsql as
$$
declare
    curb2 cursor for select *
                     from cube_b2;
    curb32 cursor for select *
                      from cube_b32;
begin
    call construct_cube_b2();

    create or replace view aggbids as
    select time, price, sum(1) as aggb3
    from bids
    group by time, price
    order by time desc, price asc;

    open curb2;
    move next from curb2;
    delete from b32;
    insert into b32
    select b3.time, b3.price, 0, (f).gbykey1, (f).aggb2 * b3.aggb3
    from aggbids b3,
         lateral (select lookup_cube_b2(b3.*, curb2) as f) func;
    close curb2;


    call construct_cube_b32();

    create or replace view b1dist as
    select distinct time
    from bids
    order by time asc;

    open curb32;
    move next from curb32;
    delete from b1resdist;
    insert into b1resdist
    select b1.*, (f).*
    from b1dist b1,
         lateral (select lookup_cube_b32(b1.*, curb32) as f) func;
    close curb32;

    delete from b1res;
    insert into b1res
    select b1.price, b1.time, b1.volume, d.agg from bids b1 join b1resdist d on b1.time = d.time;
end ;
$$;