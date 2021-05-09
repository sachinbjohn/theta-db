create table bids_temp
(
	price double precision,
	time double precision,
	volume double precision
);

alter table bids_temp owner to postgres;

create table vwap3res
(
	time double precision,
	vwap double precision
);

alter table vwap3res owner to postgres;

create table cubeb3g0
(
	time1 double precision,
	time2 double precision,
	agg double precision
);

alter table cubeb3g0 owner to postgres;

create table cubeb2g0
(
	time1 double precision,
	time2 double precision,
	price double precision,
	agg double precision
);

alter table cubeb2g0 owner to postgres;

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

create table rtb3
(
	l integer,
	tx double precision,
	ty double precision,
	v double precision,
	r integer
);

alter table rtb3 owner to postgres;

create unique index ib3x
	on rtb3 (l, tx) include (ty, v);

create unique index ib3y
	on rtb3 (l, ty) include (tx, v);

create table rtb3new
(
	l integer,
	tx double precision,
	ty double precision,
	v double precision,
	r integer
);

alter table rtb3new owner to postgres;

create table rtb2d1
(
	l1 integer,
	tx double precision,
	ty double precision,
	r1 integer
);

alter table rtb2d1 owner to postgres;

create unique index ib2tx
	on rtb2d1 (l1, tx) include (ty, r1);

create unique index ib2ty
	on rtb2d1 (l1, ty) include (tx, r1);

create table rtb2d1new
(
	l1 integer,
	tx double precision,
	ty double precision,
	r1 integer
);

alter table rtb2d1new owner to postgres;

create table rtb2d2
(
	l1 integer,
	l2 integer,
	px double precision,
	py double precision,
	v double precision,
	r1 integer,
	r2 integer
);

alter table rtb2d2 owner to postgres;

create unique index ib2py
	on rtb2d2 (l1, l2, r1, py) include (px, v);

create unique index ib2px
	on rtb2d2 (l1, l2, r1, px) include (py, v);

create table rtb2d2new
(
	l1 integer,
	l2 integer,
	px double precision,
	py double precision,
	v double precision,
	r1 integer,
	r2 integer
);

alter table rtb2d2new owner to postgres;

----------------------------
create function mergelookup_b2(_outert1 double precision, _outert2 double precision, _outerp double precision, _cur refcursor, _succ refcursor) returns double precision
	language plpgsql
as $$
declare
        _inner cubeb2g0%rowtype;
    begin
        fetch relative 0 from _succ into _inner;
        while _inner.time1 <= _outert1 or _inner.time2 >= _outert2 or _inner.price < _outerp loop
            fetch next from _succ into _inner;
            move next from _cur;
        end loop;
        fetch relative 0 from _cur into _inner;
      -- raise notice 'outer = %   inner = % %', _outerp, _innerp.price, _innerp.agg;
        if _inner.agg isnull then
            return 0;
        else
            return _inner.agg;
        end if;
    end;
$$;

alter function mergelookup_b2(double precision, double precision, double precision, refcursor, refcursor) owner to postgres;

create function mergelookup_b3(_outert1 double precision, _outert2 double precision, _cur refcursor, _succ refcursor) returns double precision
	language plpgsql
as $$
declare
        _inner cubeb3g0%rowtype;
    begin
        fetch relative 0 from _succ into _inner;
        while _inner.time1 <= _outert1 or _inner.time2 >= _outert2 loop
            fetch next from _succ into _inner;
            move next from _cur;
        end loop;
        fetch relative 0 from _cur into _inner;
      -- raise notice 'outer = %   inner = % %', _outerp, _innerp.price, _innerp.agg;
        if _inner.agg isnull then
            return 0;
        else
            return _inner.agg;
        end if;
    end;
$$;

alter function mergelookup_b3(double precision, double precision, refcursor, refcursor) owner to postgres;


create function rangelookup_b3(_t1 double precision, _t2 double precision, _levels integer) returns double precision
	language plpgsql
as $$
-- [t2: t1]
declare
    _sum  double precision := 0;
    _xmin double precision := float8 '+infinity'
    _ymax double precision := float8 '-infinity'
    _row  rtb3%ROWTYPE;
begin
    for i in reverse _levels..0
        loop
            SELECT i, min(tx), max(ty), sum(v), 0
            into _row
            from rtb3
            where l = i
              and tx >= _t2
              and tx < _xmin
              and ty <= _t1;

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
              and tx >= _t2
              and _ymax < ty
              and ty <= _t1;

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

alter function rangelookup_b3(double precision, double precision, integer) owner to postgres;

create function rangelookup_b2(_t1 double precision, _t2 double precision, _p double precision, _lt integer, _lp integer) returns double precision
	language plpgsql
as $$
declare
    _sum   double precision := 0;
    _txmin double precision := float8 '+infinity'
    _tymax double precision := float8 '-infinity'
    _pxmin double precision := float8 '+infinity'
    _pymax double precision := float8 '-infinity'
    _curd1 cursor for select *
                      from rtb2d1new;
    _rowd2 rtb2d2%ROWTYPE;
    _rowd1 rtb2d1%ROWTYPE;
begin
    for i in reverse _lt..0
        loop

            delete from  rtb2d1new;
            insert into rtb2d1new
            select *
            from rtb2d1
            where l1 = i
              and tx >= _t2
              and tx < _txmin
              and ty <= _t1;

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
                    _pxmin := float8 '+infinity';
                    _pymax := float8 '-infinity';

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

            delete from  rtb2d1new;
            insert into rtb2d1new
            select *
            from rtb2d1
            where l1 = i
              and tx >= _t2
              and ty > _tymax
              and ty <= _t1;

            open _curd1;
            fetch next from _curd1 into _rowd1;

            while _rowd1 is not null
                loop
                    _pxmin := float8 '+infinity';
                    _pymax := float8 '-infinity'; --fix?
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

alter function rangelookup_b2(double precision, double precision, double precision, integer, integer) owner to postgres;

create procedure construct_rtb3(lb3 integer, bfb3 integer)
	language plpgsql
as $$
begin
    delete from  rtb3;
    delete from  rtb3new;

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
            delete from  rtb3new;

        end loop;

end;
$$;

alter procedure construct_rtb3(integer, integer) owner to postgres;

create procedure construct_rtb2(ltb2 integer, lpb2 integer, bfb2 integer)
	language plpgsql
as $$
begin
    delete from  rtb2d1;
    delete from  rtb2d2;

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
            delete from  rtb2d1new;

            insert into rtb2d1new
            select i, min(tx), max(ty), r1 / bfb2
            from rtb2d1
            where l1 = i - 1
            group by r1 / bfb2;

            insert into rtb2d1
            select * from rtb2d1new;

            delete from  rtb2d2new;
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
                   delete from  rtb2d2new;
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

create procedure querynaive()
	language plpgsql
as $$
declare

begin
    delete  from vwap3res;
    insert into vwap3res
    select b1.time, sum(b1.price * b1.volume)
    from bids b1
    where (select sum(0.25 * b3.volume) from bids b3 where b3.time <= b1.time and b3.time >= b1.time - 10)
              <
          (select sum(b2.volume) from bids b2 where b2.price < b1.price and b2.time <= b1.time and b2.time >= b1.time - 10)
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
    succb3 cursor for select *
                     from cubeb3g0;
    curb2 cursor for select *
                     from cubeb2g0;
    succb2 cursor for select *
                     from cubeb2g0;
begin
    create or replace view prices as
    select distinct price from bids order by price;

    create or replace view times1 as
    select distinct b.time as time1 from bids b order by time1;

    create or replace view times2 as
    select distinct b.time as time2 from bids b order by time2 desc;

    create or replace view bidstar as
        select * from times1, times2, prices;

    create or replace view cubeb2g3 as
    select bs.time1, bs.time2, bs.price, sum(b.volume) as agg
    from bidstar bs left outer join bids b on
        bs.price = b.price and bs.time1 = b.time and bs.time2 = b.time - 10
    group by bs.time1, bs.time2, bs.price
    order by bs.time1, bs.time2 desc, bs.price;

    create or replace view cubeb2g2 as
    select time1, time2, price, sum(agg) over (partition by time2, price order by time1) as agg
    from cubeb2g3
    order by time1, time2 desc, price;

    create or replace view cubeb2g1 as
    select time1, time2, price, sum(agg) over (partition by time1, price order by time2 desc) as agg
    from cubeb2g2
    order by time1, time2 desc, price;

    delete from cubeb2g0;
    insert into cubeb2g0
    select time1, time2, price, sum(agg) over (partition by time1, time2 order by price ) as agg
    from cubeb2g1
    order by time1, time2 desc, price;

    create or replace view cubeb3g2 as
    select t.time1, t.time2, sum(0.25 * volume) as agg
    from (select * from times1,times2)t
             left outer join bids on
        t.time1 = bids.time and t.time2 = bids.time -10
    group by t.time1, t.time2
    order by t.time1, t.time2 desc;

    create or replace view cubeb3g1 as
    select time1, time2, sum(agg) over (partition by time2 order by time1) as agg
    from cubeb3g2
    order by time1, time2 desc;


    delete from cubeb3g0;
    insert into cubeb3g0
    select time1, time2, sum(agg) over (partition by time1 order by time2 desc) as agg
    from cubeb3g1
    order by time1, time2 desc;

    open curb3;
    open succb3;
    move next from succb3;

    delete from b1b3;
    insert into b1b3
    select b1.time, b1.price, b1.volume, mergelookup_b3(b1.time, b1.time-10, curb3, succb3)
    from (select * from bids order by time, price) b1
    order by time, price;
    close curb3;
    close succb3;

    open curb2;
    open succb2;
    move next from succb2;

    delete from b1b3b2;
    insert into b1b3b2
    select time, price, volume, aggb3, mergelookup_b2(time, time-10, price, curb2, succb2)
    from b1b3;
    close curb2;
    close succb2;

    delete from vwap3res;
    insert into vwap3res
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
    call construct_rtb3(lb3, bfb3);
    call construct_rtb2(ltb2, lpb2, bfb2);

    delete from  b1b3;
    insert into b1b3
    select b1.time, b1.price, b1.volume, rangelookup_b3(b1.time, b1.time-10, lb3)
    from bids b1;

    delete from  b1b3b2;
    insert into b1b3b2
    select time, price, volume, aggb3, rangelookup_b2(time, time -10, price, ltb2, lpb2)
    from b1b3;

    delete from  vwap3res;
    insert into vwap3res
    select time, sum(price * volume)
    from b1b3b2
    where aggb3 < aggb2
    group by time;
end;

$$;

alter procedure queryrange(integer, integer, integer, integer, integer) owner to postgres;


create procedure expt1(startp integer, endp integer, testflag integer, minutes integer)
	language plpgsql
as $$
declare
    StartTime timestamptz;
    EndTime   timestamptz;
    Delta     double precision;
    allp      integer;
    n         integer;
    p         integer;
    t         integer;
    r         integer;
    lpb2       integer;
    ltb2       integer;
    lb3       integer;
    bfb2      integer;
    bfb3      integer;
    tablename varchar;
    querystr  varchar;
    csvpath   varchar;
    enable    boolean;
    maxTimeMS integer;
begin
    for allp in startp..endp
        loop
            t := 10;
            n := allp;
            p := allp;
            r := allp;
            tablename := format('bids_%s_%s_%s_%s', n, r, p, t);
            csvpath := format('/var/data/csvdata/%s.csv', tablename);
            querystr := format('create table %s(price double precision, time double precision, volume double precision)', tablename);
            execute querystr;
            querystr := format('COPY %s FROM ''%s'' DELIMITER '','' CSV HEADER', tablename, csvpath);
            execute querystr;
        end loop;


    enable := (testflag & 1) != 0;
    maxTimeMS := minutes * 60 * 1000;
    for allp in startp..endp
        loop
            if enable
            then

                t := 10;
                n := allp;
                p := allp;
                r := allp;
                tablename := format('bids_%s_%s_%s_%s', n, r, p, t);
                querystr := format('create or replace view bids as select * from %s', tablename);
                execute querystr;

                StartTime := clock_timestamp();
                call querynaive();
                EndTime := clock_timestamp();
                Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
                RAISE NOTICE 'Q2,Naive,SQL,%,%,%,%,%', n, r, p, t, Delta;
                if (Delta > maxTimeMS)
                then
                    enable := false;
                end if;


            end if;
        end loop;

    enable := (testflag & 8) != 0;
    for allp in startp..endp
        loop
            if enable
            then

                t := 10;
                n := allp;
                p := allp;
                r := allp;
                tablename := format('bids_%s_%s_%s_%s', n, r, p, t);
                querystr := format('create or replace view bids as select * from %s', tablename);
                execute querystr;

                StartTime := clock_timestamp();
                call querymerge();
                EndTime := clock_timestamp();
                Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
                RAISE NOTICE 'Q2,Merge,SQL,%,%,%,%,%', n, r, p, t, Delta;
                if (Delta > maxTimeMS)
                then
                    enable := false;
                end if;


            end if;
        end loop;

    enable := (testflag & 4) != 0;
    for allp in startp..endp
        loop
            if enable
            then

                t := 10;
                n := allp;
                p := allp;
                r := allp;
                tablename := format('bids_%s_%s_%s_%s', n, r, p, t);
                querystr := format('create or replace view bids as select * from %s', tablename);
                execute querystr;


                bfb3 := 4;
                bfb2 := 4;
                ltb2 := 4;
                lpb2 := p/2;
                lb3 := 4;

                StartTime := clock_timestamp();
                call queryrange(lb3, ltb2, lpb2, bfb3, bfb2);
                EndTime := clock_timestamp();
                Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
                RAISE NOTICE 'Q2,Range,SQL,%,%,%,%,%', n, r, p, t, Delta;
                if (Delta > maxTimeMS)
                then
                    enable := false;
                end if;


            end if;
        end loop;
    create or replace view bids as select * from bids_temp;
    for allp in startp..endp
        loop
            t := 10;
            n := allp;
            p := allp;
            r := allp;
            tablename := format('bids_%s_%s_%s_%s', n, r, p, t);
            querystr := format('drop table %s', tablename);
            execute querystr;
        end loop;


end
$$;

alter procedure expt1(integer, integer, integer, integer) owner to postgres;
