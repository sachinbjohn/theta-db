
create table bids_temp
(
	price double precision,
	time double precision,
	volume double precision
);

alter table bids_temp owner to postgres;


create table cubeb2g0
(
	price double precision,
	agg double precision
);

alter table cubeb2g0 owner to postgres;

create table rtb2
(
	l1 integer,
	px double precision,
	py double precision,
	v double precision,
	r1 integer
);

alter table rtb2 owner to postgres;

create unique index ib2py
	on rtb2 (l1, py) include (px, v);

create unique index ib2px
	on rtb2 (l1, px) include (py, v);

create table rtb2new
(
	l1 integer,
	px double precision,
	py double precision,
	v double precision,
	r1 integer
);

alter table rtb2new owner to postgres;

create table b1b2
(
	price double precision,
	volume double precision,
	aggb2 double precision
);

alter table b1b2 owner to postgres;

---------------------

create function rangelookup_b2(_p double precision, _levels integer) returns double precision
	language plpgsql
as $$
declare
    _sum  double precision := 0;
    _xmin double precision := _p ; --fix
    _ymax double precision := 0; --fix
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

alter function rangelookup_b2(double precision, integer) owner to postgres;

create function mergelookup_b2(_outer double precision, _cur refcursor) returns double precision
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

alter function mergelookup_b2(double precision, refcursor) owner to postgres;

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
    --raise notice 'VWAP = %', vwap1res;
end;
$$;

alter procedure querynaive() owner to postgres;

create procedure querymerge()
	language plpgsql
as $$
declare
    cursorb2 cursor for select * from cubeb2g0;
    vwap1res double precision;
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


    delete from cubeb2g0;
    insert into cubeb2g0
    select price, sum(agg) over (order by price rows between unbounded preceding and 1 preceding) as agg
    from cubeb2g1
    order by price;


    open cursorb2;
    move next from cursorb2;

    delete from b1b2;
    insert into b1b2
    select price, volume, mergelookup_b2(price, cursorb2) as aggb2
    from aggbids;
    close cursorb2;

    SELECT SUM(b1b2.price * b1b2.volume)
    into vwap1res
    FROM b1b2
    WHERE (SELECT SUM(0.25 * b3.volume) FROM bids b3) < b1b2.aggb2;

    --raise notice 'VWAP = %', vwap1res;

end;
$$;

alter procedure querymerge() owner to postgres;

create procedure queryrange(lb2 integer, bfb2 integer)
	language plpgsql
as $$
declare
    vwap1res double precision;
begin

    create or replace view aggbids as
    select price, sum(volume) as volume
    from bids
    group by price;

    delete from rtb2new;
    delete from rtb2;
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

    delete from b1b2;
    insert into b1b2
    select price, volume, rangelookup_b2(price, lb2)
    from aggbids;


    SELECT SUM(b1b2.price * b1b2.volume)
    into vwap1res
    FROM b1b2
    WHERE (SELECT SUM(0.25 * b3.volume) FROM bids b3) < b1b2.aggb2;

   --raise notice 'VWAP = %', vwap1res;
end;

$$;

alter procedure queryrange(integer, integer) owner to postgres;

create procedure expt1(startp integer, endp integer, testflag integer, minutes integer)
    language plpgsql
as
$$
declare
    StartTime timestamptz;
    EndTime   timestamptz;
    Delta     double precision;
    allp      integer;
    n         integer;
    p         integer;
    t         integer;
    r         integer;
    lb2       integer;
    bfb2      integer;
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
                RAISE NOTICE 'Q1,Naive,SQL,%,%,%,%,%', n, r, p, t, Delta;
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
                RAISE NOTICE 'Q1,Merge,SQL,%,%,%,%,%', n, r, p, t, Delta;
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

                bfb2 := 256;
                lb2 := 2;
                if (p < 16) then
                    bfb2 := (1 << (p / 2));
                end if;

                StartTime := clock_timestamp();
                call queryrange(lb2, bfb2);
                EndTime := clock_timestamp();
                Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
                RAISE NOTICE 'Q1,Range,SQL,%,%,%,%,%', n, r, p, t, Delta;
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
            --execute querystr;
        end loop;


end
$$;

alter procedure expt1(integer, integer, integer, integer) owner to postgres;

