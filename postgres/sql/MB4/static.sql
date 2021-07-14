create table bids_temp
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

create procedure initnaive()
    language plpgsql as
$$
begin
    delete from result;
end;
$$;

create procedure querynaive()
    language plpgsql as
$$
begin
    insert into result
    select b1.price,
           b1.time,
           b1.volume,
           (select sum(1) from bids b2 where b2.time < b1.time and b2.price < b1.price)
    from bids b1;
end;
$$;

create procedure initsmart()
    language plpgsql as
$$
begin
    delete from result;
end;
$$;

create procedure querysmart()
    language plpgsql as
$$
begin

    create or replace view aggbids as
    select time, price, sum(1) as agg
    from bids
    group by time, price;

    create or replace view cumaggbids as
    select b1.price, b1.time, sum(b2.agg) as agg
    from aggbids b1
             left join aggbids b2 on b2.time < b1.time and b2.price < b1.price
    group by b1.price, b1.time;

    insert into result
    select b1.price, b1.time, b1.volume, agg
    from bids b1
             join cumaggbids c on b1.price = c.price and b1.time = c.time;

end;
$$;

create procedure initrange()
    language plpgsql as
$$
begin
    delete from result;
    delete from rt_b2;
    delete from rt_b2_new;
end;
$$;

create procedure queryrange(lt integer, lp integer)
    language plpgsql as
$$
begin

    create or replace view aggbids as
    select time, price, sum(1) as agg
    from bids
    group by time, price;

    call construct_rt_b2(lt, lp);
    insert into result
    select b1.*, (f).*
    from bids b1,
         lateral (select lookup_rt_b2(b1.*, lt, lp) as f offset 0) func;

end;
$$;

create procedure initmerge()
    language plpgsql as
$$
begin
    delete from result;
    delete from cube_b2;
end;
$$;

create procedure querymerge()
    language plpgsql as
$$
declare
    curb2 cursor for select *
                     from cube_b2;
begin

    create or replace view aggbids as
    select time, price, sum(1) as agg
    from bids
    group by time, price
    order by time asc, price asc;


    call construct_cube_b2();

    open curb2;
    move next from curb2;
    insert into result
    select b1.*, (f).*
    from (select * from bids order by time asc, price asc) b1,
         lateral (select lookup_cube_b2(b1.*, curb2) as f offset 0) func;
    close curb2;
end;
$$;