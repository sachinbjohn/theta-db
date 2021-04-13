create procedure querynaive()
	language plpgsql
as $$
declare

begin
    truncate vwap2res;
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
                     from cubeb3g0;
    curb2 cursor for select *
                     from cubeb2g0;
begin
    create or replace view prices as
    select distinct price from bids order by price;

    create or replace view times as
    select distinct b.time from bids b order by time;

    create or replace view cubeb2g2 as
    select pt.time, pt.price, sum(b.volume) as agg
    from (select *
          from prices,
               times) pt
             left outer join bids b on
        pt.price = b.price and pt.time = b.time
    group by pt.time, pt.price
    order by pt.time, pt.price;

    create or replace view cubeb2g1 as
    select time, price, sum(agg) over (partition by price order by time) as agg
    from cubeb2g2
    order by time, price;

    truncate cubeb2g0;
    insert into cubeb2g0
    select time, price, sum(agg) over (partition by time order by price rows between unbounded preceding and 1 preceding) as agg
    from cubeb2g1
    order by time, price;

    create or replace view cubeb3g1 as
    select times.time, sum(0.25 * volume) as agg
    from times
             left outer join bids on
        times.time = bids.time
    group by times.time;

    truncate cubeb3g0;
    insert into cubeb3g0
    select time, sum(agg) over (order by time) as agg
    from cubeb3g1
    order by time;

    open curb3;
    move next from curb3;

    truncate b1b3;
    insert into b1b3
    select b1.time, b1.price, b1.volume, mergelookup_b3(b1.time, curb3)
    from (select * from bids order by time, price) b1
    order by time, price;

    open curb2;
    move next from curb2;

    truncate b1b3b2;
    insert into b1b3b2
    select time, price, volume, aggb3, mergelookup_b2(time, price, curb2)
    from b1b3;

    truncate vwap2res;
    insert into vwap2res
    select time, sum(price * volume)
    from b1b3b2
    where aggb3 < aggb2
    group by time;

end;
$$;

alter procedure querymerge() owner to postgres;

create function mergelookup_b3(_outer dval, _cur refcursor) returns dval
	language plpgsql
as $$
declare
        _inner cubeb3g0%rowtype;
    begin
        fetch relative 0 from _cur into _inner;
        while _inner.time < _outer loop
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

alter function mergelookup_b3(dval, refcursor) owner to postgres;

create function mergelookup_b2(_outert dval, _outerp dval, _cur refcursor) returns dval
	language plpgsql
as $$
declare
        _inner cubeb2g0%rowtype;
    begin
        fetch relative 0 from _cur into _inner;
        while _inner.time < _outert or _inner.price < _outerp loop
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

alter function mergelookup_b2(dval, dval, refcursor) owner to postgres;

create function rangelookup_b3(_t dval, _levels integer) returns dval
	language plpgsql
as $$
declare
    _sum  dval := 0;
    _xmin dval := _t + 1; --fix
    _ymax dval := -1; --fix
    _row  rtb3%ROWTYPE;
begin
    for i in reverse _levels..0
        loop
            SELECT i, min(tx), max(ty), sum(v), 0
            into _row
            from rtb3
            where l = i
              and tx < _xmin
              and ty <= _t;

            if _row.v is not null then
                _sum := _sum + _row.v;
                _xmin := _row.tx;
                if _row.ty > _ymax then
                    _ymax := _row.ty;
                end if;
            end if;

            select i, min(tx), max(ty), sum(v), 0
            into _row
            from rtb3
            where l = i
              and _ymax < ty
              and ty <= _t;

            if _row.v is not null then
                _sum := _sum + _row.v;
                _ymax := _row.ty;
                if _row.tx < _xmin then
                    _xmin := _row.tx;
                end if;
            end if;
        end loop;
    return _sum;
end
$$;

alter function rangelookup_b3(dval, integer) owner to postgres;

create function rangelookup_b2(_t dval, _p dval, _lt integer, _lp integer) returns dval
	language plpgsql
as $$
declare
    _sum   dval := 0;
    _txmin dval := _t + 1; --fix
    _tymax dval := -1; --fix
    _pxmin dval := _p;
    _pymax dval := 0; --fix?
    _curd1 cursor for select *
                      from rtb2d1new;
    _rowd2 rtb2d2%ROWTYPE;
    _rowd1 rtb2d1%ROWTYPE;
begin
    for i in reverse _lt..0
        loop

            truncate rtb2d1new;
            insert into rtb2d1new
            select *
            from rtb2d1
            where l1 = i
              and tx < _txmin
              and ty <= _t;

            open _curd1;
            fetch next from _curd1 into _rowd1;

            while _rowd1 is not null
                loop
                    if _rowd1.tx < _txmin
                    then
                        _txmin := _rowd1.tx;
                    end if;
                    if _rowd1.ty > _tymax
                    then
                        _tymax := _rowd1.ty;
                    end if;
                    _pxmin := _p;
                    _pymax := 0; --fix?

                    for j in reverse _lp..0
                        loop
                            SELECT i,
                                   j,
                                   min(px),
                                   max(py),
                                   sum(v),
                                   0,
                                   0
                            into _rowd2
                            from rtb2d2
                            where l1 = i
                              and l2 = j
                              and py < _p
                              and px < _pxmin
                              and r1 = _rowd1.r1;

                            if _rowd2.v is not null then
                                _sum := _sum + _rowd2.v;
                                _pxmin := _rowd2.px;
                                if _rowd2.py > _pymax then
                                    _pymax := _rowd2.py;
                                end if;
                                --raise notice 'Q1 i=% j=% row=% tx=% ty=% px=% py=% sum=%',i, j, _rowd2, _txmin, _tymax, _pxmin, _pymax, _sum;
                            end if;

                            SELECT i,
                                   j,
                                   min(px),
                                   max(py),
                                   sum(v),
                                   0,
                                   0
                            into _rowd2
                            from rtb2d2
                            where l1 = i
                              and l2 = j
                              and py < _p
                              and py > _pymax
                              and r1 = _rowd1.r1;

                            if _rowd2.v is not null then
                                _sum := _sum + _rowd2.v;
                                _pymax := _rowd2.py;
                                if _rowd2.px < _pxmin then
                                    _pxmin := _rowd2.px;
                                end if;
                               -- raise notice 'Q2 i=% j=% row=% tx=% ty=% px=% py=% sum=%',i, j, _rowd2, _txmin, _tymax, _pxmin, _pymax, _sum;
                            end if;
                        end loop;
                    fetch next from _curd1 into _rowd1;
                end loop;
            close _curd1;

            truncate rtb2d1new;
            insert into rtb2d1new
            select *
            from rtb2d1
            where l1 = i
              and ty > _tymax
              and ty <= _t;

            open _curd1;
            fetch next from _curd1 into _rowd1;

            while _rowd1 is not null
                loop
                    _pxmin := _p;
                    _pymax := 0; --fix?
                    if _rowd1.tx < _txmin
                    then
                        _txmin := _rowd1.tx;
                    end if;
                    if _rowd1.ty > _tymax
                    then
                        _tymax := _rowd1.ty;
                    end if;
                    for j in reverse _lp..0
                        loop
                            SELECT i,
                                   j,
                                   min(px),
                                   max(py),
                                   sum(v),
                                   0,
                                   0
                            into _rowd2
                            from rtb2d2
                            where l1 = i
                              and l2 = j
                              and py < _p
                              and px < _pxmin
                              and r1 = _rowd1.r1;

                            if _rowd2.v is not null then
                                _sum := _sum + _rowd2.v;
                                _pxmin := _rowd2.px;
                                if _rowd2.py > _pymax then
                                    _pymax := _rowd2.py;
                                end if;
                                --raise notice 'Q3 i=% j=% row=% tx=% ty=% px=% py=% sum=%',i, j, _rowd2, _txmin, _tymax, _pxmin, _pymax, _sum;
                            end if;

                            SELECT i,
                                   j,
                                   min(px),
                                   max(py),
                                   sum(v),
                                   0,
                                   0
                            into _rowd2
                            from rtb2d2
                            where l1 = i
                              and l2 = j
                              and py < _p
                              and py > _pymax
                              and r1 = _rowd1.r1;

                            if _rowd2.v is not null then
                                _sum := _sum + _rowd2.v;
                                _pymax := _rowd2.py;
                                if _rowd2.px < _pxmin then
                                    _pxmin := _rowd2.px;
                                end if;
                                --raise notice 'Q4 i=% j=% row=% tx=% ty=% px=% py=% sum=%',i, j, _rowd2, _txmin, _tymax, _pxmin, _pymax, _sum;
                            end if;
                        end loop;
                    fetch next from _curd1 into _rowd1;
                end loop;
            close _curd1;

        end loop;
    return _sum;
end
$$;

alter function rangelookup_b2(dval, dval, integer, integer) owner to postgres;

create procedure construct_rtb3(lb3 integer, bfb3 integer)
	language plpgsql
as $$
begin
    truncate rtb3;
    truncate rtb3new;

    insert into rtb3
    select 0, time, time, sum(0.25 * volume), rank() over (order by time) - 1
    from bids
    group by time;

    for i in 1..lb3
        loop
            insert into rtb3new
            select i, min(tx), max(ty), sum(v), r / bfb3
            from rtb3
            where l = i - 1
            group by r / bfb3;

            insert into rtb3 select * from rtb3new;
            truncate rtb3new;

        end loop;

end;
$$;

alter procedure construct_rtb3(integer, integer) owner to postgres;

create procedure construct_rtb2(ltb2 integer, lpb2 integer, bfb2 integer)
	language plpgsql
as $$
begin
    truncate rtb2d1;
    truncate rtb2d2;

    insert into rtb2d2
    select 0,
           0,
           price,
           price,
           sum(volume),
           dense_rank() over (order by time) - 1,
           rank() over (partition by time order by price) - 1
    from bids
    group by time, price;

    insert into rtb2d1
    select 0, time, time, dense_rank() over (order by time) - 1
    from bids group by time;

    for i in 1..ltb2
        loop
            truncate rtb2d1new;

            insert into rtb2d1new
            select i, min(tx), max(ty), r1 / bfb2
            from rtb2d1
            where l1 = i - 1
            group by r1 / bfb2;

            insert into rtb2d1
            select * from rtb2d1new;

            truncate rtb2d2new;
            insert into rtb2d2new
            select i,
                   0,
                   px,
                   py,
                   sum(v),
                   r1/bfb2,
                   dense_rank() over ( partition by r1/bfb2 order by px) - 1
            from rtb2d2
            where l1 = i - 1 and l2 = 0
            group by r1/bfb2, px, py;

            insert into rtb2d2
            select *
            from rtb2d2new;


            for j in 1..lpb2
                loop
                   truncate rtb2d2new;
                    insert into rtb2d2new
                    select i,
                           j,
                           min(px),
                           max(py),
                           sum(v),
                           r1,
                           r2 / bfb2
                    from rtb2d2
                    where l2 = j - 1
                      and l1 = i
                    group by r1, r2 / bfb2;

                    insert into rtb2d2
                    select *
                    from rtb2d2new;
                end loop;
        end loop;

end;
$$;

alter procedure construct_rtb2(integer, integer, integer) owner to postgres;

create procedure queryrange(lb3 integer, ltb2 integer, lpb2 integer)
	language plpgsql
as $$
declare
    bfb3 integer := 256;
    bfb2 integer := 32;
begin
    call construct_rtb3(lb3, bfb3);
    call construct_rtb2(ltb2, lpb2, bfb2);

    truncate b1b3;
    insert into b1b3
    select b1.time, b1.price, b1.volume, rangelookup_b3(b1.time, lb3)
    from bids b1;

    truncate b1b3b2;
    insert into b1b3b2
    select time, price, volume, aggb3, rangelookup_b2(time, price, ltb2, lpb2)
    from b1b3;

    truncate vwap2res;
    insert into vwap2res
    select time, sum(price * volume)
    from b1b3b2
    where aggb3 < aggb2
    group by time;
end;

$$;

alter procedure queryrange(integer, integer, integer) owner to postgres;

