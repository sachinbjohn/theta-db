--------------------- AUTO GEN MERGE ----------------------- 
drop procedure if exists construct_cube_bS;
create procedure construct_cube_bS()
    language plpgsql as
$$
begin
    create temp table domain_bS_dim1 on commit drop as
    SELECT DISTINCT r.time AS ineqkey1
    FROM aggbidsR r
    UNION
    SELECT DISTINCT s.time AS ineqkey1
    FROM aggbidsS s
    ORDER BY ineqkey1 ASC;

    create temp table domain_bS_dim2 on commit drop as
    SELECT DISTINCT r.price AS ineqkey2
    FROM aggbidsR r
    UNION
    SELECT DISTINCT s.price AS ineqkey2
    FROM aggbidsS s
    ORDER BY ineqkey2 ASC;


    create temp table cross_bS on commit drop as
    SELECT *
    FROM domain_bS_dim1
             NATURAL JOIN domain_bS_dim2;


    create temp table cube_bS_delta2 on commit drop as
    SELECT x.ineqkey1, x.ineqkey2, SUM(s.agg) AS agg
    FROM cross_bS x
             LEFT JOIN aggbidsS s ON (x.ineqkey1 = s.time AND x.ineqkey2 = s.price)
    GROUP BY x.ineqkey1, x.ineqkey2
    ORDER BY x.ineqkey1 ASC, x.ineqkey2 ASC;

    create temp table cube_bS_delta1 on commit drop as
    SELECT ineqkey1,
           ineqkey2,
           SUM(agg)
           OVER (partition by ineqkey1 ORDER BY ineqkey2 ASC rows between unbounded preceding and 1 preceding) AS agg
    FROM cube_bS_delta2
    ORDER BY ineqkey1 ASC, ineqkey2 ASC;

    create temp table cube_bS on commit drop as
    SELECT ineqkey1,
           ineqkey2,
           SUM(agg)
           OVER (partition by ineqkey2 ORDER BY ineqkey1 ASC rows between unbounded preceding and 1 preceding) AS agg
    FROM cube_bS_delta1
    ORDER BY ineqkey1 ASC, ineqkey2 ASC;

end
$$;

drop type if exists cube_bS_aggtype cascade;
create type cube_bS_aggtype as
(
    aggbS double precision
);

drop function if exists lookup_cube_bS;
create function lookup_cube_bS(_outer record, _cursor refcursor) returns SETOF cube_bS_aggtype
    language plpgsql as
$$
declare
    _inner    record;
    _grpcount integer;
begin
    fetch relative 0 from _cursor into _inner;
    while NOT ((_inner.ineqkey1 = _outer.time AND _inner.ineqkey2 = _outer.price))
        loop
            fetch next from _cursor into _inner;
        end loop;
    _grpcount := 0;
    while (_inner.ineqkey1 = _outer.time AND _inner.ineqkey2 = _outer.price)
        loop
            if _inner.agg IS NOT NULL
            then
                return next ROW ((_inner.agg)::double precision);
            end if;
            fetch next from _cursor into _inner;
            _grpcount := (_grpcount + -1);
        end loop;
    move relative _grpcount from _cursor;
    return;
end
$$;

--------------------- AUTO GEN RANGE ----------------------- 
drop type if exists rt_bS_aggtype cascade;
create type rt_bS_aggtype as
(
    aggbS double precision
);

drop procedure if exists construct_rt_bS;
create procedure construct_rt_bS()
    language plpgsql as
$$
declare
    bf_d1  integer := 2;
    bf_d2  integer := 2;
    _count integer;
begin
    drop index if exists rt_bS_idx2;
    drop index if exists rt_bS_idx1;

    create temp table rt_bS
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
    ) on commit drop;

    create index rt_bS_idx1 on rt_bS (lvl1, upper1) include (lower1);
    create index rt_bS_idx2 on rt_bS (lvl1, lvl2, lower1, upper1, upper2) include (lower2,agg);

    insert into rt_bS
    SELECT ceil(log(2, (MAX(rnk1) OVER ( ) + 1))),
           rnk1,
           lower1,
           upper1,
           ceil(log(2, (MAX(rnk2) OVER (partition by rnk1 ) + 1))),
           rnk2,
           lower2,
           upper2,
           agg
    FROM (
             SELECT dense_rank() over ( ORDER BY time ASC ) - 1                   AS rnk1,
                    time                                                          AS lower1,
                    time                                                          AS upper1,
                    dense_rank() over (partition by time ORDER BY price ASC ) - 1 AS rnk2,
                    price                                                         AS lower2,
                    price                                                         AS upper2,
                    SUM(agg)                                                      AS agg
             FROM aggbidsS
             GROUP BY time, price
         ) sub;

    create temp table rt_bS_src on commit drop as
    SELECT *
    FROM rt_bS;
    loop
        create temp table rt_bS_dst on commit drop as
        SELECT lvl1,
               rnk1,
               lower1,
               upper1,
               ceil(log(2, (1 + MAX(rnk2) OVER (partition by rnk1 )))) AS lvl2,
               rnk2,
               lower2,
               upper2,
               agg
        FROM (
                 SELECT (lvl1 - 1)                                                               AS lvl1,
                        (rnk1 / bf_d1)                                                           AS rnk1,
                        MIN(MIN(lower1)) OVER (partition by (rnk1 / bf_d1) )                     AS lower1,
                        MAX(MAX(upper1)) OVER (partition by (rnk1 / bf_d1) )                     AS upper1,
                        dense_rank() over (partition by (rnk1 / bf_d1) ORDER BY lower2 ASC ) - 1 AS rnk2,
                        lower2                                                                   AS lower2,
                        lower2                                                                   AS upper2,
                        SUM(agg)                                                                 AS agg
                 FROM rt_bS_src
                 WHERE lvl1 > 0
                 GROUP BY lvl1, (rnk1 / bf_d1), lower2
             ) sub;

        select 1
        into _count
        from rt_bS_dst
        limit 1;
        drop table if exists rt_bS_src;
        exit when _count IS NULL;

        insert into rt_bS
        SELECT *
        FROM rt_bS_dst;

        alter table rt_bS_dst
            rename to rt_bS_src;
    end loop;
    drop table if exists rt_bS_dst;
    create temp table rt_bS_src on commit drop as
    SELECT *
    FROM rt_bS;
    loop
        create temp table rt_bS_dst on commit drop as
        SELECT lvl1,
               rnk1,
               lower1,
               upper1,
               (lvl2 - 1)                                                     AS lvl2,
               (rnk2 / bf_d2)                                                 AS rnk2,
               MIN(MIN(lower2)) OVER (partition by lvl1,rnk1,(rnk2 / bf_d2) ) AS lower2,
               MAX(MAX(upper2)) OVER (partition by lvl1,rnk1,(rnk2 / bf_d2) ) AS upper2,
               SUM(agg)                                                       AS agg
        FROM rt_bS_src
        WHERE lvl2 > 0
        GROUP BY lvl1, rnk1, lower1, upper1, lvl2, (rnk2 / bf_d2);

        select 1
        into _count
        from rt_bS_dst
        limit 1;
        drop table if exists rt_bS_src;
        exit when _count IS NULL;

        insert into rt_bS
        SELECT *
        FROM rt_bS_dst;

        alter table rt_bS_dst
            rename to rt_bS_src;
    end loop;
    drop table if exists rt_bS_dst;
    analyze rt_bS;
end
$$;

drop function if exists lookup_rt_bS;
create function lookup_rt_bS(_outer record) returns SETOF rt_bS_aggtype
    language plpgsql as
$$
declare
    _agg       double precision;
    row0       record;
    lower1_min double precision := float8 '+infinity';
    upper1_max double precision := float8 '-infinity';
    row1       record;
    v1         integer;
    lower2_min double precision := float8 '+infinity';
    upper2_max double precision := float8 '-infinity';
    row2       record;
    v2         integer;
begin

    lower1_min := float8 '+infinity';
    upper1_max := float8 '-infinity';
    v1 := 0;
    loop

        select lower1, upper1
        into row1
        from rt_bS
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

            lower2_min := float8 '+infinity';
            upper2_max := float8 '-infinity';
            v2 := 0;
            loop

                select lower2, upper2, agg
                into row2
                from rt_bS
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
                from rt_bS
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

                v2 := (v2 + 1);
                select upper2
                into row2
                from rt_bS
                where ((lvl1 = v1 AND lvl2 = v2) AND (lower1 = row1.lower1 AND upper1 = row1.upper1))
                ORDER BY upper2 ASC
                limit 1;
                exit when row2 IS NULL;
            end loop;
        end if;

        select lower1, upper1
        into row1
        from rt_bS
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

            lower2_min := float8 '+infinity';
            upper2_max := float8 '-infinity';
            v2 := 0;
            loop

                select lower2, upper2, agg
                into row2
                from rt_bS
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
                from rt_bS
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

                v2 := (v2 + 1);
                select upper2
                into row2
                from rt_bS
                where ((lvl1 = v1 AND lvl2 = v2) AND (lower1 = row1.lower1 AND upper1 = row1.upper1))
                ORDER BY upper2 ASC
                limit 1;
                exit when row2 IS NULL;
            end loop;
        end if;

        v1 := (v1 + 1);
        select upper1
        into row1
        from rt_bS
        where lvl1 = v1
        ORDER BY upper1 ASC
        limit 1;
        exit when row1 IS NULL;
    end loop;
    if _agg IS NOT NULL
    then
        return next ROW (_agg);
    end if;
    return;
end
$$;
--------------------- AUTO GEN MERGE ----------------------- 
drop procedure if exists construct_cube_bR;
create procedure construct_cube_bR()
    language plpgsql as
$$
begin
    create temp table domain_bR_dim1 on commit drop as
    SELECT DISTINCT r.time AS ineqkey1
    FROM aggbidsS r
    UNION
    SELECT DISTINCT s.time AS ineqkey1
    FROM aggbidsR s
    ORDER BY ineqkey1 DESC;

    create temp table domain_bR_dim2 on commit drop as
    SELECT DISTINCT r.price AS ineqkey2
    FROM aggbidsS r
    UNION
    SELECT DISTINCT s.price AS ineqkey2
    FROM aggbidsR s
    ORDER BY ineqkey2 DESC;


    create temp table cross_bR on commit drop as
    SELECT *
    FROM domain_bR_dim1
             NATURAL JOIN domain_bR_dim2;


    create temp table cube_bR_delta2 on commit drop as
    SELECT x.ineqkey1, x.ineqkey2, SUM(s.agg) AS agg
    FROM cross_bR x
             LEFT JOIN aggbidsR s ON (x.ineqkey1 = s.time AND x.ineqkey2 = s.price)
    GROUP BY x.ineqkey1, x.ineqkey2
    ORDER BY x.ineqkey1 DESC, x.ineqkey2 DESC;

    create temp table cube_bR_delta1 on commit drop as
    SELECT ineqkey1,
           ineqkey2,
           SUM(agg)
           OVER (partition by ineqkey1 ORDER BY ineqkey2 DESC rows between unbounded preceding and 1 preceding) AS agg
    FROM cube_bR_delta2
    ORDER BY ineqkey1 DESC, ineqkey2 DESC;

    create temp table cube_bR on commit drop as
    SELECT ineqkey1,
           ineqkey2,
           SUM(agg)
           OVER (partition by ineqkey2 ORDER BY ineqkey1 DESC rows between unbounded preceding and 1 preceding) AS agg
    FROM cube_bR_delta1
    ORDER BY ineqkey1 DESC, ineqkey2 DESC;

end
$$;

drop type if exists cube_bR_aggtype cascade;
create type cube_bR_aggtype as
(
    aggbR double precision
);

drop function if exists lookup_cube_bR;
create function lookup_cube_bR(_outer record, _cursor refcursor) returns SETOF cube_bR_aggtype
    language plpgsql as
$$
declare
    _inner    record;
    _grpcount integer;
begin
    fetch relative 0 from _cursor into _inner;
    while NOT ((_inner.ineqkey1 = _outer.time AND _inner.ineqkey2 = _outer.price))
        loop
            fetch next from _cursor into _inner;
        end loop;
    _grpcount := 0;
    while (_inner.ineqkey1 = _outer.time AND _inner.ineqkey2 = _outer.price)
        loop
            if _inner.agg IS NOT NULL
            then
                return next ROW ((_inner.agg)::double precision);
            end if;
            fetch next from _cursor into _inner;
            _grpcount := (_grpcount + -1);
        end loop;
    move relative _grpcount from _cursor;
    return;
end
$$;

--------------------- AUTO GEN RANGE ----------------------- 
drop type if exists rt_bR_aggtype cascade;
create type rt_bR_aggtype as
(
    aggbR double precision
);

drop procedure if exists construct_rt_bR;
create procedure construct_rt_bR()
    language plpgsql as
$$
declare
    bf_d1  integer := 2;
    bf_d2  integer := 2;
    _count integer;
begin
    drop index if exists rt_bR_idx2;
    drop index if exists rt_bR_idx1;

    create temp table rt_bR
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
    ) on commit drop;

    create index rt_bR_idx1 on rt_bR (lvl1, lower1) include (upper1);
    create index rt_bR_idx2 on rt_bR (lvl1, lvl2, lower1, upper1, lower2) include (upper2,agg);

    insert into rt_bR
    SELECT ceil(log(2, (MAX(rnk1) OVER ( ) + 1))),
           rnk1,
           lower1,
           upper1,
           ceil(log(2, (MAX(rnk2) OVER (partition by rnk1 ) + 1))),
           rnk2,
           lower2,
           upper2,
           agg
    FROM (
             SELECT dense_rank() over ( ORDER BY time ASC ) - 1                   AS rnk1,
                    time                                                          AS lower1,
                    time                                                          AS upper1,
                    dense_rank() over (partition by time ORDER BY price ASC ) - 1 AS rnk2,
                    price                                                         AS lower2,
                    price                                                         AS upper2,
                    SUM(agg)                                                      AS agg
             FROM aggbidsR
             GROUP BY time, price
         ) sub;

    create temp table rt_bR_src on commit drop as
    SELECT *
    FROM rt_bR;
    loop
        create temp table rt_bR_dst on commit drop as
        SELECT lvl1,
               rnk1,
               lower1,
               upper1,
               ceil(log(2, (1 + MAX(rnk2) OVER (partition by rnk1 )))) AS lvl2,
               rnk2,
               lower2,
               upper2,
               agg
        FROM (
                 SELECT (lvl1 - 1)                                                               AS lvl1,
                        (rnk1 / bf_d1)                                                           AS rnk1,
                        MIN(MIN(lower1)) OVER (partition by (rnk1 / bf_d1) )                     AS lower1,
                        MAX(MAX(upper1)) OVER (partition by (rnk1 / bf_d1) )                     AS upper1,
                        dense_rank() over (partition by (rnk1 / bf_d1) ORDER BY lower2 ASC ) - 1 AS rnk2,
                        lower2                                                                   AS lower2,
                        lower2                                                                   AS upper2,
                        SUM(agg)                                                                 AS agg
                 FROM rt_bR_src
                 WHERE lvl1 > 0
                 GROUP BY lvl1, (rnk1 / bf_d1), lower2
             ) sub;

        select 1
        into _count
        from rt_bR_dst
        limit 1;
        drop table if exists rt_bR_src;
        exit when _count IS NULL;

        insert into rt_bR
        SELECT *
        FROM rt_bR_dst;

        alter table rt_bR_dst
            rename to rt_bR_src;
    end loop;
    drop table if exists rt_bR_dst;
    create temp table rt_bR_src on commit drop as
    SELECT *
    FROM rt_bR;
    loop
        create temp table rt_bR_dst on commit drop as
        SELECT lvl1,
               rnk1,
               lower1,
               upper1,
               (lvl2 - 1)                                                     AS lvl2,
               (rnk2 / bf_d2)                                                 AS rnk2,
               MIN(MIN(lower2)) OVER (partition by lvl1,rnk1,(rnk2 / bf_d2) ) AS lower2,
               MAX(MAX(upper2)) OVER (partition by lvl1,rnk1,(rnk2 / bf_d2) ) AS upper2,
               SUM(agg)                                                       AS agg
        FROM rt_bR_src
        WHERE lvl2 > 0
        GROUP BY lvl1, rnk1, lower1, upper1, lvl2, (rnk2 / bf_d2);

        select 1
        into _count
        from rt_bR_dst
        limit 1;
        drop table if exists rt_bR_src;
        exit when _count IS NULL;

        insert into rt_bR
        SELECT *
        FROM rt_bR_dst;

        alter table rt_bR_dst
            rename to rt_bR_src;
    end loop;
    drop table if exists rt_bR_dst;
    analyze rt_bR;
end
$$;

drop function if exists lookup_rt_bR;
create function lookup_rt_bR(_outer record) returns SETOF rt_bR_aggtype
    language plpgsql as
$$
declare
    _agg       double precision;
    row0       record;
    lower1_min double precision := float8 '+infinity';
    upper1_max double precision := float8 '-infinity';
    row1       record;
    v1         integer;
    lower2_min double precision := float8 '+infinity';
    upper2_max double precision := float8 '-infinity';
    row2       record;
    v2         integer;
begin

    lower1_min := float8 '+infinity';
    upper1_max := float8 '-infinity';
    v1 := 0;
    loop

        select lower1, upper1
        into row1
        from rt_bR
        where (lvl1 = v1 AND (lower1 > _outer.time AND lower1 < lower1_min))
        ORDER BY lower1 DESC
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
            v2 := 0;
            loop

                select lower2, upper2, agg
                into row2
                from rt_bR
                where (((lvl1 = v1 AND lvl2 = v2) AND (lower1 = row1.lower1 AND upper1 = row1.upper1)) AND
                       (lower2 > _outer.price AND lower2 < lower2_min))
                ORDER BY lower2 DESC
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
                from rt_bR
                where (((lvl1 = v1 AND lvl2 = v2) AND (lower1 = row1.lower1 AND upper1 = row1.upper1)) AND
                       (lower2 > _outer.price AND lower2 > upper2_max))
                ORDER BY lower2 ASC
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

                v2 := (v2 + 1);
                select lower2
                into row2
                from rt_bR
                where ((lvl1 = v1 AND lvl2 = v2) AND (lower1 = row1.lower1 AND upper1 = row1.upper1))
                ORDER BY lower2 ASC
                limit 1;
                exit when row2 IS NULL;
            end loop;
        end if;

        select lower1, upper1
        into row1
        from rt_bR
        where (lvl1 = v1 AND (lower1 > _outer.time AND lower1 > upper1_max))
        ORDER BY lower1 ASC
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
            v2 := 0;
            loop

                select lower2, upper2, agg
                into row2
                from rt_bR
                where (((lvl1 = v1 AND lvl2 = v2) AND (lower1 = row1.lower1 AND upper1 = row1.upper1)) AND
                       (lower2 > _outer.price AND lower2 < lower2_min))
                ORDER BY lower2 DESC
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
                from rt_bR
                where (((lvl1 = v1 AND lvl2 = v2) AND (lower1 = row1.lower1 AND upper1 = row1.upper1)) AND
                       (lower2 > _outer.price AND lower2 > upper2_max))
                ORDER BY lower2 ASC
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

                v2 := (v2 + 1);
                select lower2
                into row2
                from rt_bR
                where ((lvl1 = v1 AND lvl2 = v2) AND (lower1 = row1.lower1 AND upper1 = row1.upper1))
                ORDER BY lower2 ASC
                limit 1;
                exit when row2 IS NULL;
            end loop;
        end if;

        v1 := (v1 + 1);
        select lower1
        into row1
        from rt_bR
        where lvl1 = v1
        ORDER BY lower1 ASC
        limit 1;
        exit when row1 IS NULL;
    end loop;
    if _agg IS NOT NULL
    then
        return next ROW (_agg);
    end if;
    return;
end
$$;
