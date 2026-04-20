-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile
-- Size: SF*10,000

DROP TABLE IF EXISTS supplier;

CREATE TABLE supplier (
    s_suppkey   integer        NOT NULL,
    s_name      varchar(25)    NOT NULL,
    s_address   varchar(40)    NOT NULL,
    s_nationkey integer        NOT NULL,
    s_phone     varchar(15)    NOT NULL,
    s_acctbal   decimal(15, 2) NOT NULL,
    s_comment   varchar(101)   NOT NULL,
    PRIMARY KEY (s_suppkey)
);

CREATE INDEX s_nk ON supplier (s_nationkey ASC);
