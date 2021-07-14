create table bids_temp
(
    price  double precision,
    time   double precision,
    volume double precision
);


create table b1res
(
    price  double precision,
    time   double precision,
    volume double precision,
    agg    double precision
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
    call construct_rt_b2(lp);

    delete from b32;
    insert into b32
    select b3.*, (f).*
    from bids b3,
         lateral (select lookup_rt_b2(b3.*, lp) as f) func
    where (f).gbykey1 > b3.time;

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


    open curb2;
    move next from curb2;

    delete from b32;
    insert into b32
    select b3.*, (f).*
    from (select * from bids order by price asc) b3,
         lateral (select lookup_cube_b2(b3.*, curb2) as f) func
    where (f).gbykey1 > b3.time;
    close curb2;


    call construct_cube_b32();


    open curb32;
    move next from curb32;
    delete from b1res;
    insert into b1res
    select b1.*, (f).*
    from (select * from bids order by time asc) b1,
         lateral (select lookup_cube_b32(b1.*, curb32) as f) func;
    close curb32;
end ;
$$;