create table bids_temp
(
    price  double precision,
    time   double precision,
    volume double precision
);
create table b1resdist
(
    time double precision,
    agg  double precision
);
create table b1res
(
    price  double precision,
    time   double precision,
    volume double precision,
    agg    double precision
);
create table b23
(
    price  double precision,
    time   double precision,
    volume double precision,
    b3agg  double precision
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
    call construct_rt_b3(lt, lp);

    delete from b23;
    insert into b23
    select b2.*, (f).*
    from bids b2,
         lateral (select lookup_rt_b3(b2.*, lt, lp) as f) func;

    call construct_rt_b23(lt);

    delete from b1res;
    insert into b1res
    select b1.*, (f).*
    from bids b1,
         lateral (select lookup_rt_b23(b1.*, lt) as f) func;
end;
$$;

create procedure querymerge()
    language plpgsql as
$$
declare
    curb3 cursor for select *
                     from cube_b3;
    curb23 cursor for select *
                      from cube_b23;
begin
    call construct_cube_b3();

    create or replace view aggbids as
    select time, price, sum(1) as aggb2
    from bids
    group by time, price
    order by time asc, price desc;

    open curb3;
    move next from curb3;
    delete from b23;
    insert into b23
    select b2.price, b2.time, 0, (f).aggb3 * b2.aggb2
    from aggbids b2,
         lateral (select lookup_cube_b3(b2.*, curb3) as f) func;
    close curb3;

    call construct_cube_b23();

    create or replace view b1dist as
    select distinct time
    from bids
    order by time asc;

    open curb23;
    move next from curb23;
    delete from b1resdist;
    insert into b1resdist
    select b1.time, (f).aggb23
    from b1dist b1,
         lateral (select lookup_cube_b23(b1.*, curb23) as f) func;
    close curb23;

    delete from b1res;
    insert into b1res
    select b1.price, b1.time, b1.volume, d.agg from (select * from bids order by time asc) b1 join b1resdist d on b1.time = d.time;
end;
$$;