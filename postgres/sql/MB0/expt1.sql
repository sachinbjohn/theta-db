create procedure expt1(startp integer, endp integer, testflag integer, minutes integer)
    language plpgsql
as
$$
declare
    StartTime   timestamptz;
    EndTime     timestamptz;
    Delta       double precision;
    allp        integer;
    n           integer;
    p           integer;
    t           integer;
    r           integer;
    count1      integer;
    count2      integer;
    tablename   varchar;
    querystr    varchar;
    csvpath     varchar;
    outdir      varchar;
    outpath     varchar;
    enable      boolean;
    maxTimeMS   integer;
    queryname   varchar;
    resultTable varchar;
begin

    queryname := 'MB0';
    testflag := testflag & 17; --DISABLE RANGE AND MERGE
    resultTable := 'result';
    outdir := format('/var/data/result/%s/sql', queryname);
    for allp in startp..endp
        loop
            n := allp;
            p := allp;
            r := allp;
            t := allp;
            tablename := format('bids_%s_%s_%s_%s', n, r, p, t);
            csvpath := format('/var/data/csvdata/%s.csv', tablename);

            querystr := format('create table if not exists %s(like bids_temp)', tablename);
            execute querystr;
            querystr := format('delete from %s', tablename);
            execute querystr;
            querystr := format('COPY %s FROM ''%s'' DELIMITER '','' CSV HEADER', tablename, csvpath);
            execute querystr;
            querystr := format('COPY (SELECT 1) TO PROGRAM ''mkdir -p %s/%s''', outdir, tablename);
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
                 t := allp;
                tablename := format('bids_%s_%s_%s_%s', n, r, p, t);
                querystr := format('create or replace view bids as select * from %s', tablename);
                execute querystr;

                call initnaive();

                StartTime := clock_timestamp();
                call querynaive();
                EndTime := clock_timestamp();
                Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
                RAISE NOTICE '%,Naive,SQL,%,%,%,%,%', queryname, n, r, p, t, Delta;


                querystr := format('create table if not exists %s_result_naive(like %s)', tablename, resultTable);
                execute querystr;
                querystr := format('delete from %s_result_naive', tablename);
                execute querystr;
                querystr := format('insert into %s_result_naive select * from %s', tablename, resultTable);
                execute querystr;
                outpath := format('%s/%s/naive.csv', outdir, tablename);
                querystr := format(
                        'COPY (SELECT * FROM %s_result_naive r ORDER BY r.*) TO ''%s'' DELIMITER '','' CSV HEADER',
                        tablename, outpath);
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
                 t := allp;
                tablename := format('bids_%s_%s_%s_%s', n, r, p, t);
                querystr := format('create or replace view bids as select * from %s', tablename);
                execute querystr;

                call initmerge();
                StartTime := clock_timestamp();
                call querymerge();
                EndTime := clock_timestamp();
                Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
                RAISE NOTICE '%,Merge,SQL,%,%,%,%,%', queryname, n, r, p, t, Delta;

                querystr := format('create table if not exists %s_result_merge(like %s)', tablename, resultTable);
                execute querystr;
                querystr := format('delete from %s_result_merge', tablename);
                execute querystr;
                querystr := format('insert into %s_result_merge select * from %s', tablename, resultTable);
                execute querystr;
                outpath := format('%s/%s/merge.csv', outdir, tablename);
                querystr := format(
                        'COPY (SELECT * FROM %s_result_merge r ORDER BY r.*) TO ''%s'' DELIMITER '','' CSV HEADER',
                        tablename, outpath);
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
                 t := allp;
                tablename := format('bids_%s_%s_%s_%s', n, r, p, t);
                querystr := format('create or replace view bids as select * from %s', tablename);
                execute querystr;

                call initrange();
                StartTime := clock_timestamp();
                call queryrange(t, p);
                EndTime := clock_timestamp();
                Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
                RAISE NOTICE '%,Range,SQL,%,%,%,%,%', queryname, n, r, p, t, Delta;

                querystr := format(
                        'create table if not exists %s_result_range(like %s)', tablename, resultTable);
                execute querystr;
                querystr := format('delete from %s_result_range', tablename);
                execute querystr;
                querystr := format('insert into %s_result_range select * from %s', tablename, resultTable);
                execute querystr;
                outpath := format('%s/%s/range.csv', outdir, tablename);
                querystr := format(
                        'COPY (SELECT * FROM %s_result_range r ORDER BY r.*) TO ''%s'' DELIMITER '','' CSV HEADER',
                        tablename, outpath);
                execute querystr;
                if (Delta > maxTimeMS)
                then
                    enable := false;
                end if;
            end if;
        end loop;


 enable := (testflag & 16) != 0;
    for allp in startp..endp
        loop
            if enable
            then

                n := allp;
                p := allp;
                r := allp;
                 t := allp;
                tablename := format('bids_%s_%s_%s_%s', n, r, p, t);
                querystr := format('create or replace view bids as select * from %s', tablename);
                execute querystr;

                call initsmart();
                StartTime := clock_timestamp();
                call querysmart();
                EndTime := clock_timestamp();
                Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
                RAISE NOTICE '%,Smart,SQL,%,%,%,%,%', queryname, n, r, p, t, Delta;

                querystr := format(
                        'create table if not exists %s_result_smart(like %s)', tablename, resultTable);
                execute querystr;
                querystr := format('delete from %s_result_smart', tablename);
                execute querystr;
                querystr := format('insert into %s_result_smart select * from %s', tablename, resultTable);
                execute querystr;
                outpath := format('%s/%s/smart.csv', outdir, tablename);
                querystr := format(
                        'COPY (SELECT * FROM %s_result_smart r ORDER BY r.*) TO ''%s'' DELIMITER '','' CSV HEADER',
                        tablename, outpath);
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
            t := allp;
            tablename := format('bids_%s_%s_%s_%s', n, r, p, t);

             if (testflag & 17) = 17
            then
                querystr := format(
                        'select count(*) from (select * from %s_result_naive except select * from %s_result_smart) diff',
                        tablename, tablename);
                execute querystr into count1;
                querystr := format(
                        'select count(*) from (select * from %s_result_smart except select * from %s_result_naive) diff',
                        tablename, tablename);
                execute querystr into count2;
                raise notice '%s  Naive-Smart : %, Smart-Naive : %', tablename, count1, count2;
            end if;

            if (testflag & 5) = 5
            then
                querystr := format(
                        'select count(*) from (select * from %s_result_naive except select * from %s_result_range) diff',
                        tablename, tablename);
                execute querystr into count1;
                querystr := format(
                        'select count(*) from (select * from %s_result_range except select * from %s_result_naive) diff',
                        tablename, tablename);
                execute querystr into count2;
                raise notice '%s  Naive-Range : %, Range-Naive : %', tablename, count1, count2;
            end if;

            if (testflag & 9) = 9
            then
                querystr := format(
                        'select count(*) from (select * from %s_result_naive except select * from %s_result_merge) diff',
                        tablename, tablename);
                execute querystr into count1;
                querystr := format(
                        'select count(*)  from (select * from %s_result_merge except select * from %s_result_naive) diff',
                        tablename, tablename);
                execute querystr into count2;
                raise notice '%s  Naive-Merge : %, Merge-Naive : %', tablename, count1, count2;
            end if;

        end loop;

end
$$;

alter procedure expt1(integer, integer, integer, integer) owner to postgres;

