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
    b2time double precision,
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
    select * from bids b1,
           lateral (select b2.time, sum(1) from bids b2 group by b2.time) q;
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
    select time, sum(1) as agg
    from bids
    group by time;

    insert into result
    select b1.price, b1.time, b1.volume, b2.time, b2.agg
    from bids b1, aggbids b2;
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
    select time, sum(1) as agg
    from bids
    group by time;

    call construct_rt_b2();
    insert into result
    select b1.*, (f).*
    from bids b1,
             lateral (select lookup_rt_b2(b1.*) as f offset 0) func;

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
    select time, sum(1) as agg
    from bids
    group by time
    order by time asc;

    call construct_cube_b2();

    open curb2;
    move next from curb2;
    insert into result
    select b1.*, (f).*
    from (select * from bids order by time asc) b1,
           lateral (select lookup_cube_b2(b1.*, curb2) as f offset 0) func;
    close curb2;
end;
$$;