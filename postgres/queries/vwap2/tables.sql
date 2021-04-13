create domain dval as double precision;

alter domain dval owner to postgres;



create table bids17151010
(
	price dval,
	time dval,
	volume dval
);

alter table bids17151010 owner to postgres;

create table vwap2res
(
	time dval,
	vwap dval
);

alter table vwap2res owner to postgres;

create table bids15151010
(
	price dval,
	time dval,
	volume dval
);

alter table bids15151010 owner to postgres;

create table cubeb3g0
(
	time dval,
	agg dval
);

alter table cubeb3g0 owner to postgres;

create table cubeb2g0
(
	time dval,
	price dval,
	agg dval
);

alter table cubeb2g0 owner to postgres;

create table b1b3
(
	time dval,
	price dval,
	volume dval,
	aggb3 dval
);

alter table b1b3 owner to postgres;

create table b1b3b2
(
	time dval,
	price dval,
	volume dval,
	aggb3 dval,
	aggb2 dval
);

alter table b1b3b2 owner to postgres;

create table rtb3
(
	l integer,
	tx dval,
	ty dval,
	v dval,
	r integer
);

alter table rtb3 owner to postgres;

create index ib3x
	on rtb3 (l, tx);

create index ib3y
	on rtb3 (l, ty);

create table rtb3new
(
	l integer,
	tx dval,
	ty dval,
	v dval,
	r integer
);

alter table rtb3new owner to postgres;

create table temp
(
	price dval,
	time dval,
	volume dval
);

alter table temp owner to postgres;

create table rtb2d1
(
	l1 integer,
	tx dval,
	ty dval,
	r1 integer
);

alter table rtb2d1 owner to postgres;

create index ib2tx
	on rtb2d1 (l1, tx);

create index ib2ty
	on rtb2d1 (l1, ty);

create table rtb2d1new
(
	l1 integer,
	tx dval,
	ty dval,
	r1 integer
);

alter table rtb2d1new owner to postgres;

create table rtb2d2
(
	l1 integer,
	l2 integer,
	px dval,
	py dval,
	v dval,
	r1 integer,
	r2 integer
);

alter table rtb2d2 owner to postgres;

create index ib2py
	on rtb2d2 (l1, l2, r1, py);

create index ib2px
	on rtb2d2 (l1, l2, r1, px);

create table rtb2d2new
(
	l1 integer,
	l2 integer,
	px dval,
	py dval,
	v dval,
	r1 integer,
	r2 integer
);

alter table rtb2d2new owner to postgres;

