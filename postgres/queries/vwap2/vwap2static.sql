create table bids_temp
(
	price double precision,
	time double precision,
	volume double precision
);

alter table bids_temp owner to postgres;

create table vwap2res
(
	time double precision,
	vwap double precision
);

alter table vwap2res owner to postgres;


create table b1b3dist
(
	time double precision,
	aggb3 double precision
);

create table b1b3
(
	time double precision,
	price double precision,
	volume double precision,
	aggb3 double precision
);

alter table b1b3 owner to postgres;

create table b1b3b2
(
	time double precision,
	price double precision,
	volume double precision,
	aggb3 double precision,
	aggb2 double precision
);

alter table b1b3b2 owner to postgres;

create procedure querynaive()
	language plpgsql
as $$
declare

begin
    delete  from vwap2res;
    insert into vwap2res
    select b1.time, sum(b1.price * b1.volume)
    from bids b1
    where (select sum(0.25 * b3.volume) from bids b3 where b3.time <= b1.time)
              <
          (select sum(b2.volume) from bids b2 where b2.price < b1.price and b2.time <= b1.time)
    group by b1.time;
end;
$$;

alter procedure querynaive() owner to postgres;

create procedure querymerge()
  language plpgsql
as $$
declare
    curb3 cursor for select *
                     from cube_b3;
    curb2 cursor for select *
                     from cube_b2;

begin

    call construct_cube_b2();
    call construct_cube_b3();

    open curb3;
    move next from curb3;

    delete from b1b3;
    insert into b1b3
    select b1.time, b1.price, b1.volume, (f).*
    from (select * from bids order by time, price) b1,
         lateral (select lookup_cube_b3(b1.*, curb3) as f offset 0) func
    order by time, price;
    close curb3;

    open curb2;
    move next from curb2;

    delete from b1b3b2;
    insert into b1b3b2
    select time, price, volume, aggb3, (f).*
    from b1b3, lateral (select lookup_cube_b2(b1b3.*, curb2) as f offset 0) func;

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

create procedure queryrange(lb3 integer, ltb2 integer, lpb2 integer, bfb3 integer, bfb2 integer)
  language plpgsql
as $$
declare
begin
    call construct_rt_b3(lb3, bfb3);
    call construct_rt_b2(ltb2, bfb2,  lpb2, bfb2);

    delete from  b1b3;
    insert into b1b3
    select b1.time, b1.price, b1.volume, (f).*
    from bids b1, lateral (select  lookup_rt_b3(b1.*, lb3) as f offset 0) func;

    delete from  b1b3b2;
    insert into b1b3b2
    select time, price, volume, aggb3, (f).*
    from b1b3, lateral (select lookup_rt_b2( b1b3.*, ltb2, lpb2) as f offset 0)func;

    delete from  vwap2res;
    insert into vwap2res
    select time, sum(price * volume)
    from b1b3b2
    where aggb3 < aggb2
    group by time;
end;

$$;

alter procedure queryrange(integer, integer, integer, integer, integer) owner to postgres;
