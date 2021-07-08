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
    lpb2      integer;
    ltb2      integer;
    lb3       integer;
    bfb2      integer;
    bfb3      integer;
    count1    integer;
    count2    integer;
    tablename varchar;
    querystr  varchar;
    csvpath   varchar;
    enable    boolean;
    maxTimeMS integer;
begin
    t := 10;

    for allp in startp..endp
        loop
            n := allp;
            p := allp;
            r := allp;
            tablename := format('bids_%s_%s_%s_%s', n, r, p, t);
            csvpath := format('/var/data/csvdata/%s.csv', tablename);
            querystr :=
                    format('create table if not exists %s(price double precision, time double precision, volume double precision)',
                           tablename);
            execute querystr;
            querystr := format('delete from %s', tablename);
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


                querystr :=
                        format('create table if not exists %s_result_naive(time double precision, price double precision, volume double precision, aggb3 double precision, aggb2 double precision)', tablename);
                execute querystr;
                querystr := format('delete from %s_result_naive', tablename);
                execute querystr;
                querystr := format('insert into %s_result_naive select * from b1b3b2', tablename);
                execute querystr;

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

                querystr := format('create table if not exists %s_result_merge(time double precision, price double precision, volume double precision, aggb3 double precision, aggb2 double precision)',
                                   tablename);
                execute querystr;
                querystr := format('delete from %s_result_merge', tablename);
                execute querystr;
                querystr := format('insert into %s_result_merge select * from b1b3b2', tablename);
                execute querystr;

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

                n := allp;
                p := allp;
                r := allp;
                tablename := format('bids_%s_%s_%s_%s', n, r, p, t);
                querystr := format('create or replace view bids as select * from %s', tablename);
                execute querystr;



                ltb2 := t;
                lpb2 := p;
                lb3 := t;


                StartTime := clock_timestamp();
                call queryrange(lb3, ltb2, lpb2);
                EndTime := clock_timestamp();
                Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
                RAISE NOTICE 'Q2,Range,SQL,%,%,%,%,%', n, r, p, t, Delta;

                querystr := format('create table if not exists %s_result_range(time double precision, price double precision, volume double precision, aggb3 double precision, aggb2 double precision)',
                                   tablename);
                execute querystr;
                querystr := format('delete from %s_result_range', tablename);
                execute querystr;
                querystr := format('insert into %s_result_range select * from b1b3b2', tablename);
                execute querystr;

                if (Delta > maxTimeMS)
                then
                    enable := false;
                end if;
            end if;
        end loop;



    for allp in startp..endp
        loop
            n := allp;
            p := allp;
            r := allp;
            tablename := format('bids_%s_%s_%s_%s', n, r, p, t);

            if (testflag & 5) = 5
            then
            querystr := format('select count(*) from (select * from %s_result_naive except select * from %s_result_range) diff', tablename, tablename);
            execute querystr into count1;
            querystr := format('select count(*) from (select * from %s_result_range except select * from %s_result_naive) diff', tablename, tablename);
            execute querystr into count2;
            raise notice '%s  Naive-Range : %, Range-Naive : %', tablename, count1, count2;
            end if;

            if (testflag & 9) = 9
            then
            querystr := format('select count(*) from (select * from %s_result_naive except select * from %s_result_merge) diff', tablename, tablename);
            execute querystr into count1;
            querystr := format('select count(*)  from (select * from %s_result_merge except select * from %s_result_naive) diff', tablename, tablename);
            execute querystr into count2;
            raise notice '%s  Naive-Merge : %, Merge-Naive : %', tablename, count1, count2;
            end if;

        end loop;

end
$$;

alter procedure expt1(integer, integer, integer, integer) owner to postgres;

