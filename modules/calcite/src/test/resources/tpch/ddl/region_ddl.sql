-- noinspection SqlDialectInspectionForFile
-- noinspection SqlNoDataSourceInspectionForFile
-- Size: 5

DROP TABLE IF EXISTS region;

CREATE TABLE region (
    r_regionkey integer     NOT NULL,
    r_name      varchar(25) NOT NULL,
    r_comment   varchar(152),
    PRIMARY KEY (r_regionkey)
);
