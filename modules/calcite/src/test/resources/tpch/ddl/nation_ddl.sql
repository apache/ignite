-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile
-- Size: 25

DROP TABLE IF EXISTS nation;

CREATE TABLE nation (
    n_nationkey integer     NOT NULL,
    n_name      varchar(25) NOT NULL,
    n_regionkey integer     NOT NULL,
    n_comment   varchar(152),
    PRIMARY KEY (n_nationkey)
);

CREATE INDEX n_rk ON nation (n_regionkey ASC);
