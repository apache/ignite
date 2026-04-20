-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile
-- Size: SF*6,000,000

DROP TABLE IF EXISTS lineitem;

CREATE TABLE lineitem (
    l_orderkey      integer        NOT NULL,
    l_partkey       integer        NOT NULL,
    l_suppkey       integer        NOT NULL,
    l_linenumber    integer        NOT NULL,
    l_quantity      decimal(15, 2) NOT NULL,
    l_extendedprice decimal(15, 2) NOT NULL,
    l_discount      decimal(15, 2) NOT NULL,
    l_tax           decimal(15, 2) NOT NULL,
    l_returnflag    varchar(1)     NOT NULL,
    l_linestatus    varchar(1)     NOT NULL,
    l_shipdate      date           NOT NULL,
    l_commitdate    date           NOT NULL,
    l_receiptdate   date           NOT NULL,
    l_shipinstruct  varchar(25)    NOT NULL,
    l_shipmode      varchar(10)    NOT NULL,
    l_comment       varchar(44)    NOT NULL,
    PRIMARY KEY (l_orderkey, l_linenumber)
);

CREATE INDEX l_sd ON lineitem (l_shipdate ASC);
CREATE INDEX l_cd ON lineitem (l_commitdate ASC);
CREATE INDEX l_rd ON lineitem (l_receiptdate ASC);
CREATE INDEX l_ok ON lineitem (l_orderkey ASC);
CREATE INDEX l_pk_sk ON lineitem (l_partkey ASC, l_suppkey ASC);
CREATE INDEX l_sk_pk ON lineitem (l_suppkey ASC, l_partkey ASC);
