create table bids_temp
(
    price  double precision,
    time   double precision,
    volume double precision
);

create table result
(
    price  double precision,
    time   double precision,
    volume double precision,
    agg    double precision
);

create procedure initnaive() language plpgsql as
    $$
    begin
        delete from result;
    end;
    $$;

create procedure querynaive()
    language plpgsql as
$$
    begin
        insert into result
        select b1.price, b1.time, b1.volume,
               (select sum(1) from bids b2)
        from bids b1;
    end;
$$;

create procedure initsmart() language plpgsql as
    $$
    begin
        delete from result;
    end;
    $$;

create procedure querysmart()
    language plpgsql as
$$
    declare
        agg double precision := 0;
    begin
        select sum(1) into agg from bids;

        insert into result
        select b1.price, b1.time, b1.volume, agg from bids b1;
    end;
$$;