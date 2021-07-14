--------------------- AUTO GEN MERGE ----------------------- 
drop function if exists lookup_cube_b2;
drop type if exists cube_b2_aggtype;
drop procedure if exists construct_cube_b2;
drop table if exists cube_b2;

create table cube_b2
(
    ineqkey1 double precision,
    agg      double precision
);

create procedure construct_cube_b2()
    language plpgsql as
$$
begin
    create or replace view domain_b2_dim1 as
    SELECT DISTINCT r.time AS ineqkey1
    FROM bids r
    UNION
    SELECT DISTINCT s.time AS ineqkey1
    FROM aggbids s
    ORDER BY ineqkey1 ASC;


    create or replace view cross_b2 as
    SELECT *
    FROM domain_b2_dim1;


    create or replace view cube_b2_delta1 as
    SELECT x.ineqkey1, SUM(s.agg) AS agg
    FROM cross_b2 x
             LEFT JOIN aggbids s ON x.ineqkey1 = s.time
    GROUP BY x.ineqkey1
    ORDER BY x.ineqkey1 ASC;


    delete from cube_b2;
    insert into cube_b2
    SELECT ineqkey1, SUM(agg) OVER ( ORDER BY ineqkey1 ASC rows between unbounded preceding and 1 preceding) AS agg
    FROM cube_b2_delta1
    ORDER BY ineqkey1 ASC;
end
$$;

create type cube_b2_aggtype as
(
    aggb2 double precision
);

create function lookup_cube_b2(_outer record, _cursor refcursor) returns SETOF cube_b2_aggtype
    language plpgsql as
$$
declare
    _inner    cube_b2;
    _grpcount integer;
begin
    fetch relative 0 from _cursor into _inner;
    while NOT (_inner.ineqkey1 = _outer.time)
        loop
            fetch next from _cursor into _inner;
        end loop;
    _grpcount := 0;
    while _inner.ineqkey1 = _outer.time
        loop
            return next ROW (_inner.agg);
            fetch next from _cursor into _inner;
            _grpcount := (_grpcount + -1);
        end loop;
    move relative _grpcount from _cursor;
    return;
end
$$;

--------------------- AUTO GEN RANGE ----------------------- 
drop function if exists lookup_rt_b2;
drop procedure if exists construct_rt_b2;
drop type if exists rt_b2_aggtype;
drop index if exists rt_b2_idx1;
drop table if exists rt_b2_new;
drop table if exists rt_b2;

create table rt_b2
(
    lvl1   integer,
    rnk1   integer,
    lower1 double precision,
    upper1 double precision,
    agg    double precision
);
create table rt_b2_new
(
    lvl1   integer,
    rnk1   integer,
    lower1 double precision,
    upper1 double precision,
    agg    double precision
);
create index rt_b2_idx1 on rt_b2 (lvl1, upper1) include (lower1,agg);
create type rt_b2_aggtype as
(
    aggb2 double precision
);
create procedure construct_rt_b2(height_d1 integer)
    language plpgsql as
$$
declare
    bf_d1 integer := 2;
begin
    delete from rt_b2;
    insert into rt_b2
    SELECT 0, dense_rank() over ( ORDER BY time ASC ) - 1, time, time, SUM(agg)
    FROM aggbids
    GROUP BY time;
    for v1 in 1..height_d1
        loop
            delete from rt_b2_new;
            insert into rt_b2_new
            SELECT v1,
                   (rnk1 / bf_d1),
                   MIN(MIN(lower1)) OVER (partition by (rnk1 / bf_d1) ),
                   MAX(MAX(upper1)) OVER (partition by (rnk1 / bf_d1) ),
                   SUM(agg)
            FROM rt_b2
            WHERE lvl1 = (v1 - 1)
            GROUP BY (rnk1 / bf_d1);
            insert into rt_b2
            SELECT *
            FROM rt_b2_new;
        end loop;
end
$$;
create function lookup_rt_b2(_outer record, height_d1 integer) returns SETOF rt_b2_aggtype
    language plpgsql as
$$
declare
    _agg       double precision;
    row0       record;
    lower1_min double precision := float8 '+infinity';
    upper1_max double precision := float8 '-infinity';
    row1       record;
begin


    lower1_min := float8 '+infinity';
    upper1_max := float8 '-infinity';


    for v1 in reverse height_d1..0
        loop


            select lower1, upper1, agg
            into row1
            from rt_b2
            where (lvl1 = v1 AND (upper1 < _outer.time AND upper1 < lower1_min))
            ORDER BY upper1 DESC
            limit 1;


            if row1.lower1 IS NOT NULL
            then
                if row1.lower1 < lower1_min
                then
                    lower1_min := row1.lower1;
                end if;
                if row1.upper1 > upper1_max
                then
                    upper1_max := row1.upper1;
                end if;
                if _agg IS NULL
                then
                    _agg := row1.agg;
                else
                    _agg := (_agg + row1.agg);
                end if;
            end if;


            select lower1, upper1, agg
            into row1
            from rt_b2
            where (lvl1 = v1 AND (upper1 < _outer.time AND upper1 > upper1_max))
            ORDER BY upper1 ASC
            limit 1;


            if row1.lower1 IS NOT NULL
            then
                if row1.lower1 < lower1_min
                then
                    lower1_min := row1.lower1;
                end if;
                if row1.upper1 > upper1_max
                then
                    upper1_max := row1.upper1;
                end if;
                if _agg IS NULL
                then
                    _agg := row1.agg;
                else
                    _agg := (_agg + row1.agg);
                end if;
            end if;
        end loop;
    return next ROW (_agg);
    return;
end
$$;
