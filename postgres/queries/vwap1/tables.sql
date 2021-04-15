create domain dval as double precision;

alter domain dval owner to postgres;


create table bids10101000
(
	price dval,
	time dval,
	volume dval
);

alter table bids10101000 owner to postgres;

create table bids11111100
(
	price dval,
	time dval,
	volume dval
);

alter table bids11111100 owner to postgres;

create table bids12121200
(
	price dval,
	time dval,
	volume dval
);

alter table bids12121200 owner to postgres;

create table bids13131300
(
	price dval,
	time dval,
	volume dval
);

alter table bids13131300 owner to postgres;

create table bids14141400
(
	price dval,
	time dval,
	volume dval
);

alter table bids14141400 owner to postgres;

create table bids15151500
(
	price dval,
	time dval,
	volume dval
);

alter table bids15151500 owner to postgres;

create table b1b2
(
	price dval,
	volume dval,
	aggb2 dval
);

alter table b1b2 owner to postgres;

create table cubeb2g0
(
	price dval,
	agg dval
);

alter table cubeb2g0 owner to postgres;

create table rtb2
(
	l1 integer,
	px dval,
	py dval,
	v dval,
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
	px dval,
	py dval,
	v dval,
	r1 integer
);

alter table rtb2new owner to postgres;

create table bids16161600
(
	price dval,
	time dval,
	volume dval
);

alter table bids16161600 owner to postgres;

create table bids17171700
(
	price dval,
	time dval,
	volume dval
);

alter table bids17171700 owner to postgres;

create table bids18181800
(
	price dval,
	time dval,
	volume dval
);

alter table bids18181800 owner to postgres;

create table bids19191900
(
	price dval,
	time dval,
	volume dval
);

alter table bids19191900 owner to postgres;

create table bids20202000
(
	price dval,
	time dval,
	volume dval
);

alter table bids20202000 owner to postgres;

create table bids21212100
(
	price dval,
	time dval,
	volume dval
);

alter table bids21212100 owner to postgres;

create table bids22222200
(
	price dval,
	time dval,
	volume dval
);

alter table bids22222200 owner to postgres;



create or replace view bids as select * from bids10101000;


