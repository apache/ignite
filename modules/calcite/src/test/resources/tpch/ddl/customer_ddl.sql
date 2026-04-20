-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile
-- Size: SF*150,000

DROP TABLE IF EXISTS customer;

CREATE TABLE customer (
    c_custkey    integer        NOT NULL,
    c_name       varchar(25)    NOT NULL,
    c_address    varchar(40)    NOT NULL,
    c_nationkey  integer        NOT NULL,
    c_phone      varchar(15)    NOT NULL,
    c_acctbal    decimal(15, 2) NOT NULL,
    c_mktsegment varchar(10)    NOT NULL,
    c_comment    varchar(117)   NOT NULL,
    PRIMARY KEY (c_custkey)
);

CREATE INDEX c_nk ON customer (c_nationkey ASC);
