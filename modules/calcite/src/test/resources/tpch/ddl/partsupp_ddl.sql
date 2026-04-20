-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile
-- Size: SF*800,000

DROP TABLE IF EXISTS partsupp;

CREATE TABLE partsupp (
    ps_partkey    integer        NOT NULL,
    ps_suppkey    integer        NOT NULL,
    ps_availqty   integer        NOT NULL,
    ps_supplycost decimal(15, 2) NOT NULL,
    ps_comment    varchar(199)   NOT NULL,
    PRIMARY KEY (ps_partkey, ps_suppkey)
);

CREATE INDEX ps_pk ON partsupp (ps_partkey ASC);
CREATE INDEX ps_sk_pk ON partsupp (ps_suppkey ASC, ps_partkey ASC);
