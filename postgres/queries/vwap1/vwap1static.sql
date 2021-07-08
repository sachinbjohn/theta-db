
create table bids_temp
(
	price double precision,
	time double precision,
	volume double precision
);

alter table bids_temp owner to postgres;


create table b1b2
(
	price double precision,
	volume double precision,
	aggb2 double precision
);

alter table b1b2 owner to postgres;

create procedure querynaive()
	language plpgsql
as $$
declare
  vwap1res double precision;
begin

    select sum(b1.price * b1.volume)
    into vwap1res
    from bids b1
    where (select sum(0.25 * b3.volume) from bids b3)
              <
          (select sum(b2.volume) from bids b2 where b2.price < b1.price);
    raise notice 'VWAP = %', vwap1res;
end;
$$;

alter procedure querynaive() owner to postgres;


create procedure querymerge()
    language plpgsql
as
$$
declare
    curb2 cursor for select * from cube_b2;
    vwap1res double precision;
begin

    create or replace view aggbids as
    select price, sum(volume) as volume
    from bids
    group by price
    order by price;

    call construct_cube_b2();

    open curb2;
    move next from curb2;

    delete from b1b2;
    insert into b1b2
    select price, volume, (f).aggb2
    from aggbids, lateral (select lookup_cube_b2(aggbids.*, curb2) as f offset 0) func;
    close curb2;

    SELECT SUM(b1b2.price * b1b2.volume)
    into vwap1res
    FROM b1b2
    WHERE (SELECT SUM(0.25 * b3.volume) FROM bids b3) < b1b2.aggb2;

    raise notice 'VWAP = %', vwap1res;

end;
$$;

alter procedure querymerge() owner to postgres;



create procedure queryrange(lb2 integer, bfb2 integer)
    language plpgsql
as
$$
declare
    vwap1res double precision;
begin

    create or replace view aggbids as
    select price, sum(volume) as volume
    from bids
    group by price;

    call construct_rt_b2(lb2, bfb2);
    delete from b1b2;
    insert into b1b2
    select price, volume, (f).*
    from aggbids, lateral (select lookup_rt_b2( aggbids.*, lb2) as f offset 0) func;


    SELECT SUM(b1b2.price * b1b2.volume)
    into vwap1res
    FROM b1b2
    WHERE (SELECT SUM(0.25 * b3.volume) FROM bids b3) < b1b2.aggb2;

   raise notice 'VWAP = %', vwap1res;
end;

$$;

alter procedure queryrange(integer, integer) owner to postgres;




