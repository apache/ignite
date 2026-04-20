-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile
-- Size: SF*1,500,000

DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    o_orderkey      integer        NOT NULL,
    o_custkey       integer        NOT NULL,
    o_orderstatus   varchar(1)     NOT NULL,
    o_totalprice    decimal(15, 2) NOT NULL,
    o_orderdate     date           NOT NULL,
    o_orderpriority varchar(15)    NOT NULL,
    o_clerk         varchar(15)    NOT NULL,
    o_shippriority  integer        NOT NULL,
    o_comment       varchar(79)    NOT NULL,
    PRIMARY KEY (o_orderkey)
);

CREATE INDEX o_ck ON orders (o_custkey ASC);
CREATE INDEX o_od ON orders (o_orderdate ASC);
