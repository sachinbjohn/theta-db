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
                RAISE NOTICE 'Q1,MergeAuto,SQL,%,%,%,%,%', n, r, p, t, Delta;
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

                lb2 := p;

                StartTime := clock_timestamp();
                call queryrange(lb2, 2);
                EndTime := clock_timestamp();
                Delta := 1000 * (extract(epoch from EndTime) - extract(epoch from StartTime));
                RAISE NOTICE 'Q1,RangeAuto,SQL,%,%,%,%,%', n, r, p, t, Delta;
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

