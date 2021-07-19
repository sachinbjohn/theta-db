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
    algoflag    integer;
    algoname    varchar;
    resultTable varchar;
begin

    queryname := 'MB2';
    resultTable := 'result';
    outdir := format('/var/data/result/%s', queryname);
    for allp in startp..endp
        loop
            n := allp + 1;
            r := allp;
            p := allp - 5;
            t := allp - 5;
            tablename := format('bids_%s_%s_%s_%s', n, r, p, t);
            querystr := format('COPY (SELECT 1) TO PROGRAM ''mkdir -m 777 -p %s/%s''', outdir, tablename);
            execute querystr;
        end loop;

    foreach algoflag in array array [1, 4, 8, 16]
        loop
            case algoflag
                when 1 then algoname := 'Naive';
                when 4 then algoname := 'Range';
                when 8 then algoname := 'Merge';
                when 16 then algoname := 'Smart';
                end case;

            enable := (testflag & algoflag) != 0;
            maxTimeMS := minutes * 60 * 1000;
            for allp in startp..endp
                loop
                    if enable
                    then

                        n := allp + 1;
                        r := allp;
                        p := allp - 5;
                        t := allp - 5;

                        tablename := format('bids_%s_%s_%s_%s', n, r, p, t);

                        querystr := format('call init%s()', algoname);
                        execute querystr;

                        csvpath := format('/var/data/csvdata/%s.csv', tablename);
                        querystr := format('COPY bids FROM ''%s'' DELIMITER '','' CSV HEADER', csvpath);
                        execute querystr;


                        querystr := format('select query%s(%s, %s)', algoname, p, t);
                        execute querystr into Delta;
                        RAISE NOTICE '%,%,SQL,%,%,%,%,%', queryname, algoname, n, r, p, t, Delta;

                        /*
                        querystr := format('create table if not exists %s_result_%s(like %s)', tablename, algoname, resultTable);
                        execute querystr;
                        querystr := format('delete from %s_result_%s', tablename, algoname);
                        execute querystr;
                        querystr := format('insert into %s_result_%s select * from %s', tablename, algoname,resultTable);
                        execute querystr;
                        */

                        outpath := format('%s/%s/sql-%s.csv', outdir, tablename, algoname);
                        querystr := format(
                                'COPY (SELECT * FROM %s r ORDER BY r.*) TO ''%s'' DELIMITER '','' CSV HEADER',
                                resultTable, outpath);
                        execute querystr;
                        if (Delta > maxTimeMS)
                        then
                            enable := false;
                        end if;
                    end if;
                end loop;
        end loop;
end
$$;

alter procedure expt1(integer, integer, integer, integer) owner to postgres;

