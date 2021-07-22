--------------------- AUTO GEN MERGE ----------------------- 
drop procedure if exists construct_cube_bS;
create procedure construct_cube_bS()
    language plpgsql as
$$
begin
    create temp table groups_bS on commit drop as
    SELECT DISTINCT aggbidsS.time AS gbykey1
    FROM aggbidsS;

    create temp table domain_bS_dim1 on commit drop as
    SELECT DISTINCT r.price AS ineqkey1, gbykey1
    FROM aggbidsR r,
         groups_bS
    UNION
    SELECT DISTINCT s.price AS ineqkey1, s.time AS gbykey1
    FROM aggbidsS s
    ORDER BY ineqkey1 ASC, gbykey1 ASC;


    create temp table cross_bS on commit drop as
    SELECT *
    FROM domain_bS_dim1;


    create temp table cube_bS_delta1 on commit drop as
    SELECT x.ineqkey1, x.gbykey1, SUM(s.agg) AS agg
    FROM cross_bS x
             LEFT JOIN aggbidsS s ON (x.ineqkey1 = s.price AND x.gbykey1 = s.time)
    GROUP BY x.ineqkey1, x.gbykey1
    ORDER BY x.ineqkey1 ASC, x.gbykey1 ASC;

    create temp table cube_bS on commit drop as
    SELECT ineqkey1,
           gbykey1,
           SUM(agg)
           OVER (partition by gbykey1 ORDER BY ineqkey1 ASC rows between unbounded preceding and 1 preceding) AS agg
    FROM cube_bS_delta1
    ORDER BY ineqkey1 ASC, gbykey1 ASC;

end
$$;

drop type if exists cube_bS_aggtype cascade;
create type cube_bS_aggtype as
(
    gbykey1 double precision,
    aggbS   double precision
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
    while NOT (_inner.ineqkey1 = _outer.price)
        loop
            fetch next from _cursor into _inner;
        end loop;
    _grpcount := 0;
    while _inner.ineqkey1 = _outer.price
        loop
            if _inner.agg IS NOT NULL
            then
                return next ROW (_inner.gbykey1,(_inner.agg)::double precision);
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
    gbykey1 double precision,
    aggbS   double precision
);

drop procedure if exists construct_rt_bS;
create procedure construct_rt_bS()
    language plpgsql as
$$
declare
    bf_d1  integer := 2;
    _count integer;
begin
    drop index if exists rt_bS_idx1;
    drop index if exists rt_bS_idx0;

    create temp table rt_bS
    (
        gbykey1 double precision,
        lvl1    integer,
        rnk1    integer,
        lower1  double precision,
        upper1  double precision,
        agg     double precision
    ) on commit drop;

    create index rt_bS_idx0 on rt_bS (gbykey1);
    create index rt_bS_idx1 on rt_bS (gbykey1, lvl1, upper1) include (lower1,agg);

    insert into rt_bS
    SELECT time,
           ceil(log(2, SUM(1) OVER (partition by time ))),
           dense_rank() over (partition by time ORDER BY price ASC ) - 1,
           price,
           price,
           SUM(agg)
    FROM aggbidsS
    GROUP BY time, price;

    create temp table rt_bS_src on commit drop as
    SELECT *
    FROM rt_bS;
    loop
        create temp table rt_bS_dst on commit drop as
        SELECT gbykey1,
               (lvl1 - 1)                                                   AS lvl1,
               (rnk1 / bf_d1)                                               AS rnk1,
               MIN(MIN(lower1)) OVER (partition by gbykey1,(rnk1 / bf_d1) ) AS lower1,
               MAX(MAX(upper1)) OVER (partition by gbykey1,(rnk1 / bf_d1) ) AS upper1,
               SUM(agg)                                                     AS agg
        FROM rt_bS_src
        WHERE lvl1 > 0
        GROUP BY gbykey1, lvl1, (rnk1 / bf_d1);

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
begin
    for row0 in
        SELECT DISTINCT gbykey1
        FROM rt_bS
        loop
            _agg := NULL;

            lower1_min := float8 '+infinity';
            upper1_max := float8 '-infinity';
            v1 := 1;
            loop

                select lower1, upper1, agg
                into row1
                from rt_bS
                where ((gbykey1 = row0.gbykey1 AND lvl1 = v1) AND (upper1 < _outer.price AND upper1 < lower1_min))
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
                from rt_bS
                where ((gbykey1 = row0.gbykey1 AND lvl1 = v1) AND (upper1 < _outer.price AND upper1 > upper1_max))
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

                v1 := (v1 + 1);
                select upper1
                into row1
                from rt_bS
                where (gbykey1 = row0.gbykey1 AND lvl1 = v1)
                limit 1;
                exit when row1 IS NULL;
            end loop;
            if _agg IS NOT NULL
            then
                return next ROW (row0.gbykey1,_agg);
            end if;
        end loop;
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
    SELECT DISTINCT r.price AS ineqkey1
    FROM aggbidsS r
    UNION
    SELECT DISTINCT s.price AS ineqkey1
    FROM aggbidsR s
    ORDER BY ineqkey1 DESC;


    create temp table cross_bR on commit drop as
    SELECT *
    FROM domain_bR_dim1;


    create temp table cube_bR_delta1 on commit drop as
    SELECT x.ineqkey1, SUM(s.agg) AS agg
    FROM cross_bR x
             LEFT JOIN aggbidsR s ON x.ineqkey1 = s.price
    GROUP BY x.ineqkey1
    ORDER BY x.ineqkey1 DESC;

    create temp table cube_bR on commit drop as
    SELECT ineqkey1, SUM(agg) OVER ( ORDER BY ineqkey1 DESC rows between unbounded preceding and 1 preceding) AS agg
    FROM cube_bR_delta1
    ORDER BY ineqkey1 DESC;

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
    while NOT (_inner.ineqkey1 = _outer.price)
        loop
            fetch next from _cursor into _inner;
        end loop;
    _grpcount := 0;
    while _inner.ineqkey1 = _outer.price
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
    _count integer;
begin
    drop index if exists rt_bR_idx1;

    create temp table rt_bR
    (
        lvl1   integer,
        rnk1   integer,
        lower1 double precision,
        upper1 double precision,
        agg    double precision
    ) on commit drop;

    create index rt_bR_idx1 on rt_bR (lvl1, lower1) include (upper1,agg);

    insert into rt_bR
    SELECT ceil(log(2, SUM(1) OVER ( ))), dense_rank() over ( ORDER BY price ASC ) - 1, price, price, SUM(agg)
    FROM aggbidsR
    GROUP BY price;

    create temp table rt_bR_src on commit drop as
    SELECT *
    FROM rt_bR;
    loop
        create temp table rt_bR_dst on commit drop as
        SELECT (lvl1 - 1)                                           AS lvl1,
               (rnk1 / bf_d1)                                       AS rnk1,
               MIN(MIN(lower1)) OVER (partition by (rnk1 / bf_d1) ) AS lower1,
               MAX(MAX(upper1)) OVER (partition by (rnk1 / bf_d1) ) AS upper1,
               SUM(agg)                                             AS agg
        FROM rt_bR_src
        WHERE lvl1 > 0
        GROUP BY lvl1, (rnk1 / bf_d1);

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
begin

    lower1_min := float8 '+infinity';
    upper1_max := float8 '-infinity';
    v1 := 1;
    loop

        select lower1, upper1, agg
        into row1
        from rt_bR
        where (lvl1 = v1 AND (lower1 > _outer.price AND lower1 < lower1_min))
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
            if _agg IS NULL
            then
                _agg := row1.agg;
            else
                _agg := (_agg + row1.agg);
            end if;
        end if;

        select lower1, upper1, agg
        into row1
        from rt_bR
        where (lvl1 = v1 AND (lower1 > _outer.price AND lower1 > upper1_max))
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
            if _agg IS NULL
            then
                _agg := row1.agg;
            else
                _agg := (_agg + row1.agg);
            end if;
        end if;

        v1 := (v1 + 1);
        select lower1
        into row1
        from rt_bR
        where lvl1 = v1
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
