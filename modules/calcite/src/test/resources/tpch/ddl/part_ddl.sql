-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile
-- Size: SF*200,000

DROP TABLE IF EXISTS part;

CREATE TABLE part (
    p_partkey     integer        NOT NULL,
    p_name        varchar(55)    NOT NULL,
    p_mfgr        varchar(25)    NOT NULL,
    p_brand       varchar(10)    NOT NULL,
    p_type        varchar(25)    NOT NULL,
    p_size        integer        NOT NULL,
    p_container   varchar(10)    NOT NULL,
    p_retailprice decimal(15, 2) NOT NULL,
    p_comment     varchar(23)    NOT NULL,
    PRIMARY KEY (p_partkey)
);
