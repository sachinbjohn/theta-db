--------------------- AUTO GEN MERGE ----------------------- 
drop function if exists lookup_cube_b2;
drop type if exists cube_b2_aggtype;
drop procedure if exists construct_cube_b2;
drop table if exists cube_b2;

create table cube_b2
(
    ineqkey1 double precision,
    ineqkey2 double precision,
    agg      double precision
);

create procedure construct_cube_b2()
    language plpgsql as
$$
begin
    create or replace view domain_b2_dim1 as
    SELECT r.time AS ineqkey1
    FROM bids r
    UNION
    SELECT s.time AS ineqkey1
    FROM bids s
    ORDER BY ineqkey1 ASC;


    create or replace view domain_b2_dim2 as
    SELECT r.price AS ineqkey2
    FROM bids r
    UNION
    SELECT s.price AS ineqkey2
    FROM bids s
    ORDER BY ineqkey2 ASC;


    create or replace view cross_b2 as
    SELECT *
    FROM domain_b2_dim1
             NATURAL JOIN domain_b2_dim2;


    create or replace view cube_b2_delta2 as
    SELECT x.ineqkey1, x.ineqkey2, SUM(s.volume) AS agg
    FROM cross_b2 x
             LEFT JOIN bids s ON (x.ineqkey1 = s.time AND x.ineqkey2 = s.price)
    GROUP BY x.ineqkey1, x.ineqkey2
    ORDER BY x.ineqkey1 ASC, x.ineqkey2 ASC;


    create or replace view cube_b2_delta1 as
    SELECT ineqkey1,
           ineqkey2,
           SUM(agg)
           OVER (partition by ineqkey1 ORDER BY ineqkey2 ASC rows between unbounded preceding and 1 preceding) AS agg
    FROM cube_b2_delta2
    ORDER BY ineqkey1 ASC, ineqkey2 ASC;


    delete from cube_b2;
    insert into cube_b2
    SELECT ineqkey1, ineqkey2, SUM(agg) OVER (partition by ineqkey2 ORDER BY ineqkey1 ASC ) AS agg
    FROM cube_b2_delta1
    ORDER BY ineqkey1 ASC, ineqkey2 ASC;
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
    _inner cube_b2;
begin
    fetch relative 0 from _cursor into _inner;
    while NOT ((_inner.ineqkey1 = _outer.time AND _inner.ineqkey2 = _outer.price))
        loop
            fetch next from _cursor into _inner;
        end loop;
    while (_inner.ineqkey1 = _outer.time AND _inner.ineqkey2 = _outer.price)
        loop
            return next ROW (_inner.agg);
            fetch next from _cursor into _inner;
        end loop;
    return;
end
$$;

--------------------- AUTO GEN RANGE ----------------------- 
drop function if exists lookup_rt_b2;
drop procedure if exists construct_rt_b2;
drop type if exists rt_b2_aggtype;
drop index if exists rt_b2_idx1;
drop index if exists rt_b2_idx2;
drop table if exists rt_b2_new;
drop table if exists rt_b2;

create table rt_b2
(
    lvl1   integer,
    rnk1   integer,
    lower1 double precision,
    upper1 double precision,
    lvl2   integer,
    rnk2   integer,
    lower2 double precision,
    upper2 double precision,
    agg    double precision
);
create table rt_b2_new
(
    lvl1   integer,
    rnk1   integer,
    lower1 double precision,
    upper1 double precision,
    lvl2   integer,
    rnk2   integer,
    lower2 double precision,
    upper2 double precision,
    agg    double precision
);
create index rt_b2_idx1 on rt_b2 (lvl1, upper1) include (lower1);
create index rt_b2_idx2 on rt_b2 (lvl1, lvl2, lower1, upper1, upper2) include (lower2,agg);
create type rt_b2_aggtype as
(
    aggb2 double precision
);
create procedure construct_rt_b2(height_d1 integer, bf_d1 integer, height_d2 integer, bf_d2 integer)
    language plpgsql as
$$
begin
    delete from rt_b2;
    insert into rt_b2
    SELECT 0,
           dense_rank() over ( ORDER BY time ASC ) - 1,
           time,
           time,
           0,
           dense_rank() over (partition by time ORDER BY price ASC ) - 1,
           price,
           price,
           SUM(volume)
    FROM bids
    GROUP BY time, price;
    for v1 in 1..height_d1
        loop
            delete from rt_b2_new;
            insert into rt_b2_new
            SELECT v1,
                   (rnk1 / bf_d1),
                   MIN(MIN(lower1)) OVER (partition by (rnk1 / bf_d1) ),
                   MAX(MAX(upper1)) OVER (partition by (rnk1 / bf_d1) ),
                   0,
                   dense_rank() over (partition by (rnk1 / bf_d1) ORDER BY lower2 ASC ) - 1,
                   lower2,
                   lower2,
                   SUM(agg)
            FROM rt_b2
            WHERE lvl1 = (v1 - 1)
            GROUP BY (rnk1 / bf_d1), lower2;
            insert into rt_b2
            SELECT *
            FROM rt_b2_new;
        end loop;
    for v2 in 1..height_d2
        loop
            delete from rt_b2_new;
            insert into rt_b2_new
            SELECT lvl1,
                   rnk1,
                   lower1,
                   upper1,
                   v2,
                   (rnk2 / bf_d2),
                   MIN(MIN(lower2)) OVER (partition by lvl1,rnk1,(rnk2 / bf_d2) ),
                   MAX(MAX(upper2)) OVER (partition by lvl1,rnk1,(rnk2 / bf_d2) ),
                   SUM(agg)
            FROM rt_b2
            WHERE lvl2 = (v2 - 1)
            GROUP BY lvl1, rnk1, lower1, upper1, (rnk2 / bf_d2);
            insert into rt_b2
            SELECT *
            FROM rt_b2_new;
        end loop;
end
$$;
create function lookup_rt_b2(_outer record, height_d1 integer, height_d2 integer) returns SETOF rt_b2_aggtype
    language plpgsql as
$$
declare
    _agg       double precision := 0;
    row0       record;
    lower1_min double precision := float8 '+infinity';
    upper1_max double precision := float8 '-infinity';
    row1       record;
    lower2_min double precision := float8 '+infinity';
    upper2_max double precision := float8 '-infinity';
    row2       record;
begin


    lower1_min := float8 '+infinity';
    upper1_max := float8 '-infinity';


    for v1 in reverse height_d1..0
        loop


            select lower1, upper1
            into row1
            from rt_b2
            where (lvl1 = v1 AND (upper1 <= _outer.time AND upper1 < lower1_min))
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


                lower2_min := float8 '+infinity';
                upper2_max := float8 '-infinity';


                for v2 in reverse height_d2..0
                    loop


                        select lower2, upper2, agg
                        into row2
                        from rt_b2
                        where (((lvl1 = v1 AND lvl2 = v2) AND (lower1 = row1.lower1 AND upper1 = row1.upper1)) AND
                               (upper2 < _outer.price AND upper2 < lower2_min))
                        ORDER BY upper2 DESC
                        limit 1;


                        if row2.lower2 IS NOT NULL
                        then
                            if row2.lower2 < lower2_min
                            then
                                lower2_min := row2.lower2;
                            end if;
                            if row2.upper2 > upper2_max
                            then
                                upper2_max := row2.upper2;
                            end if;
                            if _agg IS NULL
                            then
                                _agg := row2.agg;
                            else
                                _agg := (_agg + row2.agg);
                            end if;
                        end if;


                        select lower2, upper2, agg
                        into row2
                        from rt_b2
                        where (((lvl1 = v1 AND lvl2 = v2) AND (lower1 = row1.lower1 AND upper1 = row1.upper1)) AND
                               (upper2 < _outer.price AND upper2 > upper2_max))
                        ORDER BY upper2 ASC
                        limit 1;


                        if row2.lower2 IS NOT NULL
                        then
                            if row2.lower2 < lower2_min
                            then
                                lower2_min := row2.lower2;
                            end if;
                            if row2.upper2 > upper2_max
                            then
                                upper2_max := row2.upper2;
                            end if;
                            if _agg IS NULL
                            then
                                _agg := row2.agg;
                            else
                                _agg := (_agg + row2.agg);
                            end if;
                        end if;
                    end loop;
            end if;


            select lower1, upper1
            into row1
            from rt_b2
            where (lvl1 = v1 AND (upper1 <= _outer.time AND upper1 > upper1_max))
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


                lower2_min := float8 '+infinity';
                upper2_max := float8 '-infinity';


                for v2 in reverse height_d2..0
                    loop


                        select lower2, upper2, agg
                        into row2
                        from rt_b2
                        where (((lvl1 = v1 AND lvl2 = v2) AND (lower1 = row1.lower1 AND upper1 = row1.upper1)) AND
                               (upper2 < _outer.price AND upper2 < lower2_min))
                        ORDER BY upper2 DESC
                        limit 1;


                        if row2.lower2 IS NOT NULL
                        then
                            if row2.lower2 < lower2_min
                            then
                                lower2_min := row2.lower2;
                            end if;
                            if row2.upper2 > upper2_max
                            then
                                upper2_max := row2.upper2;
                            end if;
                            if _agg IS NULL
                            then
                                _agg := row2.agg;
                            else
                                _agg := (_agg + row2.agg);
                            end if;
                        end if;


                        select lower2, upper2, agg
                        into row2
                        from rt_b2
                        where (((lvl1 = v1 AND lvl2 = v2) AND (lower1 = row1.lower1 AND upper1 = row1.upper1)) AND
                               (upper2 < _outer.price AND upper2 > upper2_max))
                        ORDER BY upper2 ASC
                        limit 1;


                        if row2.lower2 IS NOT NULL
                        then
                            if row2.lower2 < lower2_min
                            then
                                lower2_min := row2.lower2;
                            end if;
                            if row2.upper2 > upper2_max
                            then
                                upper2_max := row2.upper2;
                            end if;
                            if _agg IS NULL
                            then
                                _agg := row2.agg;
                            else
                                _agg := (_agg + row2.agg);
                            end if;
                        end if;
                    end loop;
            end if;
        end loop;
    return next ROW (_agg);
    return;
end
$$;
--------------------- AUTO GEN MERGE ----------------------- 
drop function if exists lookup_cube_b3;
drop type if exists cube_b3_aggtype;
drop procedure if exists construct_cube_b3;
drop table if exists cube_b3;

create table cube_b3
(
    ineqkey1 double precision,
    agg      double precision
);

create procedure construct_cube_b3()
    language plpgsql as
$$
begin
    create or replace view domain_b3_dim1 as
    SELECT r.time AS ineqkey1
    FROM bids r
    UNION
    SELECT s.time AS ineqkey1
    FROM bids s
    ORDER BY ineqkey1 ASC;


    create or replace view cross_b3 as
    SELECT *
    FROM domain_b3_dim1;


    create or replace view cube_b3_delta1 as
    SELECT x.ineqkey1, SUM((0.25 * s.volume)) AS agg
    FROM cross_b3 x
             LEFT JOIN bids s ON x.ineqkey1 = s.time
    GROUP BY x.ineqkey1
    ORDER BY x.ineqkey1 ASC;


    delete from cube_b3;
    insert into cube_b3
    SELECT ineqkey1, SUM(agg) OVER ( ORDER BY ineqkey1 ASC ) AS agg
    FROM cube_b3_delta1
    ORDER BY ineqkey1 ASC;
end
$$;

create type cube_b3_aggtype as
(
    aggb3 double precision
);

create function lookup_cube_b3(_outer record, _cursor refcursor) returns SETOF cube_b3_aggtype
    language plpgsql as
$$
declare
    _inner cube_b3;
begin
    fetch relative 0 from _cursor into _inner;
    while NOT (_inner.ineqkey1 = _outer.time)
        loop
            fetch next from _cursor into _inner;
        end loop;
    while _inner.ineqkey1 = _outer.time
        loop
            return next ROW (_inner.agg);
            fetch next from _cursor into _inner;
        end loop;
    return;
end
$$;

--------------------- AUTO GEN RANGE ----------------------- 
drop function if exists lookup_rt_b3;
drop procedure if exists construct_rt_b3;
drop type if exists rt_b3_aggtype;
drop index if exists rt_b3_idx1;
drop table if exists rt_b3_new;
drop table if exists rt_b3;

create table rt_b3
(
    lvl1   integer,
    rnk1   integer,
    lower1 double precision,
    upper1 double precision,
    agg    double precision
);
create table rt_b3_new
(
    lvl1   integer,
    rnk1   integer,
    lower1 double precision,
    upper1 double precision,
    agg    double precision
);
create index rt_b3_idx1 on rt_b3 (lvl1, upper1) include (lower1,agg);
create type rt_b3_aggtype as
(
    aggb3 double precision
);
create procedure construct_rt_b3(height_d1 integer, bf_d1 integer)
    language plpgsql as
$$
begin
    delete from rt_b3;
    insert into rt_b3
    SELECT 0, dense_rank() over ( ORDER BY time ASC ) - 1, time, time, SUM((0.25 * volume))
    FROM bids
    GROUP BY time;
    for v1 in 1..height_d1
        loop
            delete from rt_b3_new;
            insert into rt_b3_new
            SELECT v1,
                   (rnk1 / bf_d1),
                   MIN(MIN(lower1)) OVER (partition by (rnk1 / bf_d1) ),
                   MAX(MAX(upper1)) OVER (partition by (rnk1 / bf_d1) ),
                   SUM(agg)
            FROM rt_b3
            WHERE lvl1 = (v1 - 1)
            GROUP BY (rnk1 / bf_d1);
            insert into rt_b3
            SELECT *
            FROM rt_b3_new;
        end loop;
end
$$;
create function lookup_rt_b3(_outer record, height_d1 integer) returns SETOF rt_b3_aggtype
    language plpgsql as
$$
declare
    _agg       double precision := 0;
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
            from rt_b3
            where (lvl1 = v1 AND (upper1 <= _outer.time AND upper1 < lower1_min))
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
            from rt_b3
            where (lvl1 = v1 AND (upper1 <= _outer.time AND upper1 > upper1_max))
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
