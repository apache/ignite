/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
;
drop schema if exists pg_catalog cascade;
create schema pg_catalog;

drop alias if exists pg_convertType;
create alias pg_convertType deterministic for "org.h2.server.pg.PgServer.convertType";

drop alias if exists pg_get_oid;
create alias pg_get_oid deterministic for "org.h2.server.pg.PgServer.getOid";

create table pg_catalog.pg_version as select 2 as version, 2 as version_read;
grant select on pg_catalog.pg_version to PUBLIC;

create view pg_catalog.pg_roles -- (oid, rolname, rolcreaterole, rolcreatedb)
as
select
    id oid,
    cast(name as varchar_ignorecase) rolname,
    case when admin then 't' else 'f' end as rolcreaterole,
    case when admin then 't' else 'f' end as rolcreatedb
from INFORMATION_SCHEMA.users;
grant select on pg_catalog.pg_roles to PUBLIC;

create view pg_catalog.pg_namespace -- (oid, nspname)
as
select
    id oid,
    cast(schema_name as varchar_ignorecase) nspname
from INFORMATION_SCHEMA.schemata;
grant select on pg_catalog.pg_namespace to PUBLIC;

create table pg_catalog.pg_type(
    oid int primary key,
    typname varchar_ignorecase,
    typnamespace int,
    typlen int,
    typtype varchar,
    typbasetype int,
    typtypmod int,
    typnotnull boolean,
    typinput varchar
);
grant select on pg_catalog.pg_type to PUBLIC;

insert into pg_catalog.pg_type
select
    pg_convertType(data_type) oid,
    cast(type_name as varchar_ignorecase) typname,
    (select oid from pg_catalog.pg_namespace where nspname = 'pg_catalog') typnamespace,
    -1 typlen,
    'c' typtype,
    0 typbasetype,
    -1 typtypmod,
    false typnotnull,
    null typinput
from INFORMATION_SCHEMA.type_info
where pos = 0
    and pg_convertType(data_type) <> 705; -- not unknown

merge into pg_catalog.pg_type values(
    19,
    'name',
    (select oid from pg_catalog.pg_namespace where nspname = 'pg_catalog'),
    -1,
    'c',
    0,
    -1,
    false,
    null
);
merge into pg_catalog.pg_type values(
    0,
    'null',
    (select oid from pg_catalog.pg_namespace where nspname = 'pg_catalog'),
    -1,
    'c',
    0,
    -1,
    false,
    null
);
merge into pg_catalog.pg_type values(
    22,
    'int2vector',
    (select oid from pg_catalog.pg_namespace where nspname = 'pg_catalog'),
    -1,
    'c',
    0,
    -1,
    false,
    null
);
merge into pg_catalog.pg_type values(
    2205,
    'regproc',
    (select oid from pg_catalog.pg_namespace where nspname = 'pg_catalog'),
    4,
    'b',
    0,
    -1,
    false,
    null
);

create domain regproc as varchar_ignorecase;

create view pg_catalog.pg_class -- (oid, relname, relnamespace, relkind, relam, reltuples, reltablespace, relpages, relhasindex, relhasrules, relhasoids, relchecks, reltriggers)
as
select
    id oid,
    cast(table_name as varchar_ignorecase) relname,
    (select id from INFORMATION_SCHEMA.schemata where schema_name = table_schema) relnamespace,
    case table_type when 'TABLE' then 'r' else 'v' end relkind,
    0 relam,
    cast(0 as float) reltuples,
    0 reltablespace,
    0 relpages,
    false relhasindex,
    false relhasrules,
    false relhasoids,
    cast(0 as smallint) relchecks,
    (select count(*) from INFORMATION_SCHEMA.triggers t where t.table_schema = table_schema and t.table_name = table_name) reltriggers
from INFORMATION_SCHEMA.tables
union all
select
    id oid,
    cast(index_name as varchar_ignorecase) relname,
    (select id from INFORMATION_SCHEMA.schemata where schema_name = table_schema) relnamespace,
    'i' relkind,
    0 relam,
    cast(0 as float) reltuples,
    0 reltablespace,
    0 relpages,
    true relhasindex,
    false relhasrules,
    false relhasoids,
    cast(0 as smallint) relchecks,
    0 reltriggers
from INFORMATION_SCHEMA.indexes;
grant select on pg_catalog.pg_class to PUBLIC;

create table pg_catalog.pg_proc(
    oid int,
    proname varchar_ignorecase,
    prorettype int,
    pronamespace int
);
grant select on pg_catalog.pg_proc to PUBLIC;

create table pg_catalog.pg_trigger(
    oid int,
    tgconstrrelid int,
    tgfoid int,
    tgargs int,
    tgnargs int,
    tgdeferrable boolean,
    tginitdeferred boolean,
    tgconstrname varchar_ignorecase,
    tgrelid int
);
grant select on pg_catalog.pg_trigger to PUBLIC;

create view pg_catalog.pg_attrdef -- (oid, adsrc, adrelid, adnum)
as
select
    id oid,
    0 adsrc,
    0 adrelid,
    0 adnum,
    null adbin
from INFORMATION_SCHEMA.tables where 1=0;
grant select on pg_catalog.pg_attrdef to PUBLIC;

create view pg_catalog.pg_attribute -- (oid, attrelid, attname, atttypid, attlen, attnum, atttypmod, attnotnull, attisdropped, atthasdef)
as
select
    t.id*10000 + c.ordinal_position oid,
    t.id attrelid,
    c.column_name attname,
    pg_convertType(data_type) atttypid,
    case when numeric_precision > 255 then -1 else numeric_precision end attlen,
    c.ordinal_position attnum,
    -1 atttypmod,
    case c.is_nullable when 'YES' then false else true end attnotnull,
    false attisdropped,
    false atthasdef
from INFORMATION_SCHEMA.tables t, INFORMATION_SCHEMA.columns c
where t.table_name = c.table_name
and t.table_schema = c.table_schema
union all
select
    1000000 + t.id*10000 + c.ordinal_position oid,
    i.id attrelid,
    c.column_name attname,
    pg_convertType(data_type) atttypid,
    case when numeric_precision > 255 then -1 else numeric_precision end attlen,
    c.ordinal_position attnum,
    -1 atttypmod,
    case c.is_nullable when 'YES' then false else true end attnotnull,
    false attisdropped,
    false atthasdef
from INFORMATION_SCHEMA.tables t, INFORMATION_SCHEMA.indexes i, INFORMATION_SCHEMA.columns c
where t.table_name = i.table_name
and t.table_schema = i.table_schema
and t.table_name = c.table_name
and t.table_schema = c.table_schema;
grant select on pg_catalog.pg_attribute to PUBLIC;

create view pg_catalog.pg_index -- (oid, indexrelid, indrelid, indisclustered, indisunique, indisprimary, indexprs, indkey, indpred)
as
select
    i.id oid,
    i.id indexrelid,
    t.id indrelid,
    false indisclustered,
    not non_unique indisunique,
    primary_key indisprimary,
    cast('' as varchar_ignorecase) indexprs,
    cast(1 as array) indkey,
    null indpred
from INFORMATION_SCHEMA.indexes i, INFORMATION_SCHEMA.tables t
where i.table_schema = t.table_schema
and i.table_name = t.table_name
and i.ordinal_position = 1
-- workaround for MS Access problem opening tables with primary key
and 1=0;
grant select on pg_catalog.pg_index to PUBLIC;

drop alias if exists pg_get_indexdef;
create alias pg_get_indexdef for "org.h2.server.pg.PgServer.getIndexColumn";

drop alias if exists pg_catalog.pg_get_indexdef;
create alias pg_catalog.pg_get_indexdef for "org.h2.server.pg.PgServer.getIndexColumn";

drop alias if exists pg_catalog.pg_get_expr;
create alias pg_catalog.pg_get_expr for "org.h2.server.pg.PgServer.getPgExpr";

drop alias if exists pg_catalog.format_type;
create alias pg_catalog.format_type for "org.h2.server.pg.PgServer.formatType";

drop alias if exists version;
create alias version for "org.h2.server.pg.PgServer.getVersion";

drop alias if exists current_schema;
create alias current_schema for "org.h2.server.pg.PgServer.getCurrentSchema";

drop alias if exists pg_encoding_to_char;
create alias pg_encoding_to_char for "org.h2.server.pg.PgServer.getEncodingName";

drop alias if exists pg_postmaster_start_time;
create alias pg_postmaster_start_time for "org.h2.server.pg.PgServer.getStartTime";

drop alias if exists pg_get_userbyid;
create alias pg_get_userbyid for "org.h2.server.pg.PgServer.getUserById";

drop alias if exists has_database_privilege;
create alias has_database_privilege for "org.h2.server.pg.PgServer.hasDatabasePrivilege";

drop alias if exists has_table_privilege;
create alias has_table_privilege for "org.h2.server.pg.PgServer.hasTablePrivilege";

drop alias if exists currtid2;
create alias currtid2 for "org.h2.server.pg.PgServer.getCurrentTid";

create table pg_catalog.pg_database(
    oid int,
    datname varchar_ignorecase,
    encoding int,
    datlastsysoid int,
    datallowconn boolean,
    datconfig array, -- text[]
    datacl array, -- aclitem[]
    datdba int,
    dattablespace int
);
grant select on pg_catalog.pg_database to PUBLIC;

insert into pg_catalog.pg_database values(
    0, -- oid
    'postgres', -- datname
    6, -- encoding, UTF8
    100000, -- datlastsysoid
    true, -- datallowconn
    null, -- datconfig
    null, -- datacl
    select min(id) from INFORMATION_SCHEMA.users where admin=true, -- datdba
    0 -- dattablespace
);

create table pg_catalog.pg_tablespace(
    oid int,
    spcname varchar_ignorecase,
    spclocation varchar_ignorecase,
    spcowner int,
    spcacl array -- aclitem[]
);
grant select on pg_catalog.pg_tablespace to PUBLIC;

insert into pg_catalog.pg_tablespace values(
    0,
    'main', -- spcname
    '?', -- spclocation
    0, -- spcowner,
    null -- spcacl
);

create table pg_catalog.pg_settings(
    oid int,
    name varchar_ignorecase,
    setting varchar_ignorecase
);
grant select on pg_catalog.pg_settings to PUBLIC;

insert into pg_catalog.pg_settings values
(0, 'autovacuum', 'on'),
(1, 'stats_start_collector', 'on'),
(2, 'stats_row_level', 'on');

create view pg_catalog.pg_user -- oid, usename, usecreatedb, usesuper
as
select
    id oid,
    cast(name as varchar_ignorecase) usename,
    true usecreatedb,
    true usesuper
from INFORMATION_SCHEMA.users;
grant select on pg_catalog.pg_user to PUBLIC;

create table pg_catalog.pg_authid(
    oid int,
    rolname varchar_ignorecase,
    rolsuper boolean,
    rolinherit boolean,
    rolcreaterole boolean,
    rolcreatedb boolean,
    rolcatupdate boolean,
    rolcanlogin boolean,
    rolconnlimit boolean,
    rolpassword boolean,
    rolvaliduntil timestamp, -- timestamptz
    rolconfig array -- text[]
);
grant select on pg_catalog.pg_authid to PUBLIC;

create table pg_catalog.pg_am(oid int, amname varchar_ignorecase);
grant select on pg_catalog.pg_am to PUBLIC;
insert into  pg_catalog.pg_am values(0, 'btree');
insert into  pg_catalog.pg_am values(1, 'hash');

create table pg_catalog.pg_description -- (objoid, objsubid, classoid, description)
as
select
    oid objoid,
    0 objsubid,
    -1 classoid,
    cast(datname as varchar_ignorecase) description
from pg_catalog.pg_database;
grant select on pg_catalog.pg_description to PUBLIC;

create table pg_catalog.pg_group -- oid, groname
as
select
    0 oid,
    cast('' as varchar_ignorecase) groname
from pg_catalog.pg_database where 1=0;
grant select on pg_catalog.pg_group to PUBLIC;

create table pg_catalog.pg_inherits(
    inhrelid int,
    inhparent int,
    inhseqno int
);
grant select on pg_catalog.pg_inherits to PUBLIC;
