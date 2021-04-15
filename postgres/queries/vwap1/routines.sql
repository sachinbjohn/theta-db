create function mergelookup_b2(_outer dval, _cur refcursor) returns dval
	language plpgsql
as $$
declare
        _inner cubeb2g0%rowtype;
    begin
        fetch relative 0 from _cur into _inner;
        while _inner.price < _outer loop
            fetch next from _cur into _inner;
        end loop;
      -- raise notice 'outer = %   inner = % %', _outerp, _innerp.price, _innerp.agg;
        if _inner.agg isnull then
            return 0;
        else
            return _inner.agg;
        end if;
    end;
$$;

alter function mergelookup_b2(dval, refcursor) owner to postgres;

create function rangelookup_b2(_p dval, _levels integer) returns dval
	language plpgsql
as $$
declare
    _sum  dval := 0;
    _xmin dval := _p ; --fix
    _ymax dval := 0; --fix
    _row  rtb2%ROWTYPE;
begin
    for i in reverse _levels..0
        loop
            SELECT i, min(px), max(py), sum(v), 0
            into _row
            from rtb2
            where l1 = i
              and px < _xmin
              and py < _p;

            if _row.v is not null then
                _sum := _sum + _row.v;
                _xmin := _row.px;
                if _row.py > _ymax then
                    _ymax := _row.py;
                end if;
            end if;

            select i, min(px), max(py), sum(v), 0
            into _row
            from rtb2
            where l1 = i
              and _ymax < py
              and py < _p;

            if _row.v is not null then
                _sum := _sum + _row.v;
                _ymax := _row.py;
                if _row.px < _xmin then
                    _xmin := _row.px;
                end if;
            end if;
        end loop;
    return _sum;
end
$$;

alter function rangelookup_b2(dval, integer) owner to postgres;

create procedure construct_rtb2(lb2 integer, bfb2 integer)
	language plpgsql
as $$
begin
    truncate rtb2;
    truncate rtb2new;

    insert into rtb2
    select 0, price, price, sum(volume), rank() over (order by price) - 1
    from aggbids
    group by price;

    for i in 1..lb2
        loop
            insert into rtb2new
            select i, min(px), max(py), sum(v), r1 / bfb2
            from rtb2
            where l1 = i - 1
            group by r1 / bfb2;

            insert into rtb2 select * from rtb2new;
            truncate rtb2new;

        end loop;

end;
$$;

alter procedure construct_rtb2(integer, integer) owner to postgres;

create procedure querynaive()
	language plpgsql
as $$
declare
  vwap1res dval;
begin

    select sum(b1.price * b1.volume)
    into vwap1res
    from bids b1
    where (select sum(0.25 * b3.volume) from bids b3)
              <
          (select sum(b2.volume) from bids b2 where b2.price < b1.price);
    --raise notice 'VWAP = %', vwap1res;
end;
$$;

alter procedure querynaive() owner to postgres;

create procedure querymerge()
	language plpgsql
as $$
declare
    curb2 cursor for select *
                     from cubeb2g0;
    vwap1res dval;
begin
    create or replace view prices as
    select distinct price from bids order by price;


    create or replace view aggbids as
    select price, sum(volume) as volume
    from bids
    group by price
    order by price;

    create or replace view cubeb2g1 as
    select p.price, sum(b.volume) as agg
    from prices p
             left outer join aggbids b on
        p.price = b.price
    group by p.price
    order by p.price;

    truncate cubeb2g0;
    insert into cubeb2g0
    select price, sum(agg) over (order by price rows between unbounded preceding and 1 preceding) as agg
    from cubeb2g1
    order by price;


    open curb2;
    move next from curb2;

    truncate b1b2;
    insert into b1b2
    select price, volume, mergelookup_b2(price, curb2)
    from aggbids;

    SELECT SUM(b1b2.price * b1b2.volume)
    into vwap1res
    FROM b1b2
    WHERE (SELECT SUM(0.25 * b3.volume) FROM bids b3) < b1b2.aggb2;

   -- raise notice 'VWAP = %', vwap1res;

end;
$$;

alter procedure querymerge() owner to postgres;

create procedure queryrange(lb2 integer, bfb2 integer)
	language plpgsql
as $$
declare
 vwap1res dval;
begin

    create or replace view aggbids as
      select price, sum(volume) as volume from bids
      group by price;

    call construct_rtb2(lb2, bfb2);


    truncate b1b2;
    insert into b1b2
    select  price, volume, rangelookup_b2(price, lb2)
    from aggbids;

    SELECT SUM(b1b2.price * b1b2.volume)
    into vwap1res
    FROM b1b2
    WHERE (SELECT SUM(0.25 * b3.volume) FROM bids b3) < b1b2.aggb2;

     --raise notice 'VWAP = %', vwap1res;
end;

$$;

alter procedure queryrange(integer, integer) owner to postgres;

