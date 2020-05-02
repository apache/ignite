/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.sql;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.H2PooledConnection;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.table.Column;
import org.h2.value.Value;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridQueryParsingTest extends AbstractIndexingCommonTest {
    /** */
    private static Ignite ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.setCacheConfiguration(
            cacheConfiguration(DEFAULT_CACHE_NAME, "SCH1", String.class, Person.class),
            cacheConfiguration("addr", "SCH2", String.class, Address.class),
            cacheConfiguration("aff", "SCH3", PersonKey.class, Person.class));

        return c;
    }

    /**
     * @param name Cache name.
     * @param clsK Key class.
     * @param clsV Value class.
     * @return Cache configuration.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration cacheConfiguration(@NotNull String name, String sqlSchema, Class<?> clsK,
        Class<?> clsV) {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setName(name);
        cc.setCacheMode(CacheMode.PARTITIONED);
        cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cc.setNearConfiguration(null);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setRebalanceMode(SYNC);
        cc.setSqlSchema(sqlSchema);
        cc.setSqlFunctionClasses(GridQueryParsingTest.class);
        cc.setIndexedTypes(clsK, clsV);

        if (!QueryUtils.isSqlType(clsK))
            cc.setKeyConfiguration(new CacheKeyConfiguration(clsK));

        return cc;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        ignite = null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testParseSelectAndUnion() throws Exception {
        checkQuery("select 1 from Person p where addrIds in ((1,2,3), (3,4,5))");
        checkQuery("select 1 from Person p where addrId in ((1,))");
        checkQuery("select 1 from Person p " +
            "where p.addrId in (select a.id from sch2.Address a)");
        checkQuery("select 1 from Person p " +
            "where exists(select 1 from sch2.Address a where p.addrId = a.id)");
        checkQuery("select 42");
        checkQuery("select ()");
        checkQuery("select (1)");
        checkQuery("select (1 + 1)");
        checkQuery("select (1,)");
        checkQuery("select (?)");
        checkQuery("select (?,)");
        checkQuery("select (1, 2)");
        checkQuery("select (?, ? + 1, 2 + 2) as z");
        checkQuery("select (1,(1,(1,(1,(1,?)))))");
        checkQuery("select (select 1)");
        checkQuery("select (select 1, select ?)");
        checkQuery("select ((select 1), select ? + ?)");
        checkQuery("select CURRENT_DATE");
        checkQuery("select CURRENT_DATE()");

        checkQuery("select extract(year from ?)");
        checkQuery("select convert(?, timestamp)");

        checkQuery("select * from table(id bigint = 1)");
        checkQuery("select * from table(id bigint = (1))");
        checkQuery("select * from table(id bigint = (1,))");
        checkQuery("select * from table(id bigint = (1,), name varchar = 'asd')");
        checkQuery("select * from table(id bigint = (1,2), name varchar = 'asd')");
        checkQuery("select * from table(id bigint = (1,2), name varchar = ('asd',))");
        checkQuery("select * from table(id bigint = (1,2), name varchar = ?)");
        checkQuery("select * from table(id bigint = (1,2), name varchar = (?,))");
        checkQuery("select * from table(id bigint = ?, name varchar = ('abc', 'def', 100, ?)) t");

        checkQuery("select ? limit ? offset ?");

        checkQuery("select cool1()");
        checkQuery("select cool1() z");

        checkQuery("select b,a from table0('aaa', 100)");
        checkQuery("select * from table0('aaa', 100)");
        checkQuery("select * from table0('aaa', 100) t0");
        checkQuery("select x.a, y.b from table0('aaa', 100) x natural join table0('bbb', 100) y");
        checkQuery("select * from table0('aaa', 100) x join table0('bbb', 100) y on x.a=y.a and x.b = 1");
        checkQuery("select * from table0('aaa', 100) x left join table0('bbb', 100) y on x.a=y.a and x.b = 1");
        checkQuery("select * from table0('aaa', 100) x left join table0('bbb', 100) y on x.a=y.a where x.b = 1");
        checkQuery("select * from table0('aaa', 100) x left join table0('bbb', 100) y where x.b = 1");

        checkQuery("select avg(old) from Person left join sch2.Address on Person.addrId = Address.id " +
            "where lower(Address.street) = lower(?)");

        checkQuery("select avg(old) from sch1.Person join sch2.Address on Person.addrId = Address.id " +
            "where lower(Address.street) = lower(?)");

        checkQuery("select avg(old) from Person left join sch2.Address where Person.addrId = Address.id " +
            "and lower(Address.street) = lower(?)");
        checkQuery("select avg(old) from Person right join sch2.Address where Person.addrId = Address.id " +
            "and lower(Address.street) = lower(?)");

        checkQuery("select avg(old) from Person, sch2.Address where Person.addrId = Address.id " +
            "and lower(Address.street) = lower(?)");

        checkQuery("select name, name, date, date d from Person");
        checkQuery("select distinct name, date from Person");
        checkQuery("select * from Person p");
        checkQuery("select * from Person");
        checkQuery("select distinct * from Person");
        checkQuery("select p.name, date from Person p");
        checkQuery("select p.name, date from Person p for update");

        checkQuery("select * from Person p, sch2.Address a");
        checkQuery("select * from Person, sch2.Address");
        checkQuery("select p.* from Person p, sch2.Address a");
        checkQuery("select person.* from Person, sch2.Address a");
        checkQuery("select p.*, street from Person p, sch2.Address a");
        checkQuery("select p.name, a.street from Person p, sch2.Address a");
        checkQuery("select p.name, a.street from sch2.Address a, Person p");
        checkQuery("select distinct p.name, a.street from Person p, sch2.Address a");
        checkQuery("select distinct name, street from Person, sch2.Address group by old");
        checkQuery("select distinct name, street from Person, sch2.Address");
        checkQuery("select p1.name, a2.street from Person p1, sch2.Address a1, Person p2, sch2.Address a2");

        checkQuery("select p.name n, a.street s from Person p, sch2.Address a");
        checkQuery("select p.name, 1 as i, 'aaa' s from Person p");

        checkQuery("select p.name + 'a', 1 * 3 as i, 'aaa' s, -p.old, -p.old as old from Person p");
        checkQuery("select p.name || 'a' + p.name, (p.old * 3) % p.old - p.old / p.old, p.name = 'aaa', " +
            " p.name is p.name, p.old > 0, p.old >= 0, p.old < 0, p.old <= 0, p.old <> 0, p.old is not p.old, " +
            " p.old is null, p.old is not null " +
            " from Person p");

        checkQuery("select p.name from Person p where name <> 'ivan'");
        checkQuery("select p.name from Person p where name like 'i%'");
        checkQuery("select p.name from Person p where name regexp 'i%'");
        checkQuery("select p.name from Person p, sch2.Address a " +
            "where p.name <> 'ivan' and a.id > 10 or not (a.id = 100)");

        checkQuery("select case p.name when 'a' then 1 when 'a' then 2 end as a from Person p");
        checkQuery("select case p.name when 'a' then 1 when 'a' then 2 else -1 end as a from Person p");

        checkQuery("select abs(p.old)  from Person p");
        checkQuery("select cast(p.old as numeric(10, 2)) from Person p");
        checkQuery("select cast(p.old as numeric(10, 2)) z from Person p");
        checkQuery("select cast(p.old as numeric(10, 2)) as z from Person p");

        checkQuery("select * from Person p where p.name in ('a', 'b', '_' + RAND())"); // test ConditionIn
        checkQuery("select * from Person p where p.name in ('a', 'b', 'c')"); // test ConditionInConstantSet
        // test ConditionInConstantSet
        checkQuery("select * from Person p where p.name in (select a.street from sch2.Address a)");

        // test ConditionInConstantSet
        checkQuery("select (select a.street from sch2.Address a where a.id = p.addrId) from Person p");

        checkQuery("select p.name, ? from Person p where name regexp ? and p.old < ?");

        checkQuery("select count(*) as a from Person having a > 10");
        checkQuery("select count(*) as a, count(p.*), count(p.name) from Person p");
        checkQuery("select count(distinct p.name) from Person p");
        checkQuery("select name, count(*) cnt from Person group by name order by cnt desc limit 10");

        checkQuery("select p.name, avg(p.old), max(p.old) from Person p group by p.name");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by n");

        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.addrId, p.name");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name, p.addrId");
        checkQuery("select p.name n, max(p.old) + min(p.old) / count(distinct p.old) from Person p group by p.name");
        checkQuery("select p.name n, max(p.old) maxOld, min(p.old) minOld from Person p " +
            "group by p.name having maxOld > 10 and min(p.old) < 1");

        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name order by n");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name order by p.name");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name order by p.name, m");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p " +
            "group by p.name order by p.name, max(p.old) desc");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p " +
            "group by p.name order by p.name nulls first");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p " +
            "group by p.name order by p.name nulls last");
        checkQuery("select p.name n from Person p order by p.old + 10");
        checkQuery("select p.name n from Person p order by p.old + 10, p.name");
        checkQuery("select p.name n from Person p order by p.old + 10, p.name desc");

        checkQuery("select p.name n from Person p, (select a.street from sch2.Address a " +
            "where a.street is not null) ");
        checkQuery("select street from Person p, (select a.street from sch2.Address a " +
            "where a.street is not null) ");
        checkQuery("select addr.street from Person p, (select a.street from sch2.Address a " +
            "where a.street is not null) addr");

        checkQuery("select p.name n from sch1.Person p order by p.old + 10");

        checkQuery("select case when p.name is null then 'Vasya' end x from sch1.Person p");
        checkQuery("select case when p.name like 'V%' then 'Vasya' else 'Other' end x from sch1.Person p");
        checkQuery("select case when upper(p.name) = 'VASYA' then 'Vasya' " +
            "when p.name is not null then p.name else 'Other' end x from sch1.Person p");

        checkQuery("select case p.name when 'Vasya' then 1 end z from sch1.Person p");
        checkQuery("select case p.name when 'Vasya' then 1 when 'Petya' then 2 end z from sch1.Person p");
        checkQuery("select case p.name when 'Vasya' then 1 when 'Petya' then 2 else 3 end z from sch1.Person p");
        checkQuery("select case p.name when 'Vasya' then 1 else 3 end z from sch1.Person p");

        checkQuery("select count(*) as a from Person union select count(*) as a from sch2.Address");
        checkQuery("select old, count(*) as a from Person group by old union select 1, count(*) as a " +
            "from sch2.Address");
        checkQuery("select name from Person MINUS select street from sch2.Address");
        checkQuery("select name from Person EXCEPT select street from sch2.Address");
        checkQuery("select name from Person INTERSECT select street from sch2.Address");
        checkQuery("select name from Person UNION select street from sch2.Address limit 5");
        checkQuery("select name from Person UNION select street from sch2.Address limit ?");
        checkQuery("select name from Person UNION select street from sch2.Address limit ? offset ?");
        checkQuery("(select name from Person limit 4) " +
            "UNION (select street from sch2.Address limit 1) limit ? offset ?");
        checkQuery("(select 2 a) union all (select 1) order by 1");
        checkQuery("(select 2 a) union all (select 1) order by a desc nulls first limit ? offset ?");

        checkQuery("select public.\"#\".\"@\" from (select 1 as \"@\") \"#\"");
//        checkQuery("select sch.\"#\".\"@\" from (select 1 as \"@\") \"#\""); // Illegal query.
        checkQuery("select \"#\".\"@\" from (select 1 as \"@\") \"#\"");
        checkQuery("select \"@\" from (select 1 as \"@\") \"#\"");
        checkQuery("select sch1.\"#\".old from sch1.Person \"#\"");
        checkQuery("select sch1.\"#\".old from Person \"#\"");
        checkQuery("select \"#\".old from Person \"#\"");
        checkQuery("select old from Person \"#\"");
//        checkQuery("select Person.old from Person \"#\""); // Illegal query.
        checkQuery("select \"#\".* from Person \"#\"");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUseIndexHints() throws Exception {
        checkQuery("select * from Person use index (\"PERSON_NAME_IDX\")");
        checkQuery("select * from Person use index (\"PERSON_PARENTNAME_IDX\")");
        checkQuery("select * from Person use index (\"PERSON_NAME_IDX\", \"PERSON_PARENTNAME_IDX\")");
        checkQuery("select * from Person use index ()");

        checkQuery("select * from Person p use index (\"PERSON_NAME_IDX\")");
        checkQuery("select * from Person p use index (\"PERSON_PARENTNAME_IDX\")");
        checkQuery("select * from Person p use index (\"PERSON_NAME_IDX\", \"PERSON_PARENTNAME_IDX\")");
        checkQuery("select * from Person p use index ()");
    }

    /**
     * Query AST transformation heavily depends on this behavior.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testParseTableFilter() throws Exception {
        Prepared prepared = parse("select Person.old, p1.old, p1.addrId from Person, Person p1 " +
            "where exists(select 1 from sch2.Address a where a.id = p1.addrId)");

        GridSqlSelect select = (GridSqlSelect)new GridSqlQueryParser(false, log).parse(prepared);

        GridSqlJoin join = (GridSqlJoin)select.from();

        GridSqlTable tbl1 = (GridSqlTable)join.leftTable();
        GridSqlAlias tbl2Alias = (GridSqlAlias)join.rightTable();
        GridSqlTable tbl2 = tbl2Alias.child();

        // Must be distinct objects, even if it is the same table.
        assertNotSame(tbl1, tbl2);
        assertNotNull(tbl1.dataTable());
        assertNotNull(tbl2.dataTable());
        assertSame(tbl1.dataTable(), tbl2.dataTable());

        GridSqlColumn col1 = (GridSqlColumn)select.column(0);
        GridSqlColumn col2 = (GridSqlColumn)select.column(1);

        assertSame(tbl1, col1.expressionInFrom());

        // Alias in FROM must be included in column.
        assertSame(tbl2Alias, col2.expressionInFrom());

        // In EXISTS we must correctly reference the column from the outer query.
        GridSqlAst exists = select.where();
        GridSqlSubquery subqry = exists.child();
        GridSqlSelect subSelect = subqry.child();

        GridSqlColumn p1AddrIdCol = (GridSqlColumn)select.column(2);

        assertEquals("ADDRID", p1AddrIdCol.column().getName());
        assertSame(tbl2Alias, p1AddrIdCol.expressionInFrom());

        GridSqlColumn p1AddrIdColExists = subSelect.where().child(1);
        assertEquals("ADDRID", p1AddrIdCol.column().getName());

        assertSame(tbl2Alias, p1AddrIdColExists.expressionInFrom());
    }

    /** */
    @Test
    public void testParseInsert() throws Exception {
        /* Plain rows w/functions, operators, defaults, and placeholders. */
        checkQuery("insert into Person(old, name) values(5, 'John')");
        checkQuery("insert into Person(name) values(null)");
        checkQuery("insert into Person() values()");
        checkQuery("insert into Person(name) values(null), (null)");
        checkQuery("insert into Person(name) values(null),");
        checkQuery("insert into Person(name, parentName) values(null, null), (?, ?)");
        checkQuery("insert into Person(old, name) values(5, 'John',), (6, 'Jack')");
        checkQuery("insert into Person(old, name) values(5 * 3, null,)");
        checkQuery("insert into Person(old, name) values(ABS(-8), 'Max')");
        checkQuery("insert into Person(old, name) values(5, 'Jane'), (null, null), (6, 'Jill')");
        checkQuery("insert into Person(old, name, parentName) values(8 * 7, null, 'Unknown')");
        checkQuery("insert into Person(old, name, parentName) values" +
            "(2016 - 1828, CONCAT('Leo', 'Tolstoy'), CONCAT(?, 'Tolstoy'))," +
            "(?, 'AlexanderPushkin', null)," +
            "(ABS(1821 - 2016), CONCAT('Fyodor', null, UPPER(CONCAT(SQRT(?), 'dostoevsky'))), null),");
        checkQuery("insert into Person(date, old, name, parentName, addrId) values " +
            "('20160112', 1233, 'Ivan Ivanov', 'Peter Ivanov', 123)");
        checkQuery("insert into Person(date, old, name, parentName, addrId) values " +
            "(CURRENT_DATE(), RAND(), ASCII('Hi'), INSERT('Leo Tolstoy', 4, 4, 'Max'), ASCII('HI'))");
        checkQuery("insert into Person(date, old, name, parentName, addrId) values " +
            "(TRUNCATE(TIMESTAMP '2015-12-31 23:59:59'), POWER(3,12), NULL, NULL, NULL)");
        checkQuery("insert into Person SET old = 5, name = 'John'");
        checkQuery("insert into Person SET name = CONCAT('Fyodor', null, UPPER(CONCAT(SQRT(?), 'dostoevsky'))), " +
            "old = select (5, 6)");
        checkQuery("insert into Person(old, name) select ASCII(parentName), INSERT(parentName, 4, 4, 'Max') from " +
            "Person where date='2011-03-12'");

        /* Subqueries. */
        checkQuery("insert into Person(old, name) select old, parentName from Person");
        checkQuery("insert into Person(old, name) direct sorted select old, parentName from Person");
        checkQuery("insert into Person(old, name) sorted select old, parentName from Person where old > 5");
        checkQuery("insert into Person(old, name) select 5, 'John'");
        checkQuery("insert into Person(old, name) select p1.old, 'Name' from person p1 join person p2 on " +
            "p2.name = p1.parentName where p2.old > 30");
        checkQuery("insert into Person(old) select 5 from Person UNION select street from sch2.Address limit ? " +
            "offset ?");
    }

    /** */
    @Test
    public void testParseDelete() throws Exception {
        checkQuery("delete from Person");
        checkQuery("delete from Person p where p.old > ?");
        checkQuery("delete from Person where old in (select (40, 41, 42))");
        checkQuery("delete top 5 from Person where old in (select (40, 41, 42))");
        checkQuery("delete top ? from Person where old > 5 and length(name) < ?");
        checkQuery("delete from Person where name in ('Ivan', 'Peter') limit 20");
        checkQuery("delete from Person where name in ('Ivan', ?) limit ?");
    }

    /** */
    @Test
    public void testParseUpdate() throws Exception {
        checkQuery("update Person set name='Peter'");
        checkQuery("update Person per set name='Peter', old = 5");
        checkQuery("update Person p set name='Peter' limit 20");
        checkQuery("update Person p set name='Peter', old = length('zzz') limit 20");
        checkQuery("update Person p set name=? where old >= ? and old < ? limit ?");
        checkQuery("update Person p set name=(select a.Street from sch2.Address a where a.id=p.addrId), old = " +
            "(select 42) where old = sqrt(?)");
        checkQuery("update Person p set (name, old) = (select 'Peter', 42)");
        checkQuery("update Person p set (name, old) = (select street, id from sch2.Address where id > 5 and id <= ?)");
    }

    /**
     *
     */
    @Test
    public void testParseCreateIndex() throws Exception {
        assertCreateIndexEquals(
            buildCreateIndex(null, "Person", "sch1", false, QueryIndexType.SORTED,
            QueryIndex.DFLT_INLINE_SIZE,"name", true),
            "create index on Person (name)");

        assertCreateIndexEquals(
            buildCreateIndex("idx", "Person", "sch1", false, QueryIndexType.SORTED,
            QueryIndex.DFLT_INLINE_SIZE, "name", true),
            "create index idx on Person (name ASC)");

        assertCreateIndexEquals(
            buildCreateIndex("idx", "Person", "sch1", false, QueryIndexType.GEOSPATIAL,
            QueryIndex.DFLT_INLINE_SIZE, "name", true),
            "create spatial index sch1.idx on sch1.Person (name ASC)");

        assertCreateIndexEquals(
            buildCreateIndex("idx", "Person", "sch1", true, QueryIndexType.SORTED,
            QueryIndex.DFLT_INLINE_SIZE, "name", true),
            "create index if not exists sch1.idx on sch1.Person (name)");

        // When we specify schema for the table and don't specify it for the index, resulting schema is table's
        assertCreateIndexEquals(
            buildCreateIndex("idx", "Person", "sch1", true, QueryIndexType.SORTED,
            QueryIndex.DFLT_INLINE_SIZE,"name", false),
            "create index if not exists idx on sch1.Person (name dEsC)");

        assertCreateIndexEquals(
            buildCreateIndex("idx", "Person", "sch1", true, QueryIndexType.GEOSPATIAL,
            QueryIndex.DFLT_INLINE_SIZE, "old", true, "name", false),
            "create spatial index if not exists idx on Person (old, name desc)");

        // Schemas for index and table must match
        assertParseThrows("create index if not exists sch2.idx on sch1.Person (name)",
            DbException.class, "Schema name must match");

        assertParseThrows("create hash index if not exists idx on Person (name)",
            IgniteSQLException.class, "Only SPATIAL modifier is supported for CREATE INDEX");

        assertParseThrows("create unique index if not exists idx on Person (name)",
            IgniteSQLException.class, "Only SPATIAL modifier is supported for CREATE INDEX");

        assertParseThrows("create primary key on Person (name)",
            IgniteSQLException.class, "Only SPATIAL modifier is supported for CREATE INDEX");

        assertParseThrows("create primary key hash on Person (name)",
            IgniteSQLException.class, "Only SPATIAL modifier is supported for CREATE INDEX");

        assertParseThrows("create index on Person (name nulls first)",
            IgniteSQLException.class, "NULLS FIRST and NULLS LAST modifiers are not supported for index columns");

        assertParseThrows("create index on Person (name desc nulls last)",
            IgniteSQLException.class, "NULLS FIRST and NULLS LAST modifiers are not supported for index columns");
    }

    /**
     *
     */
    @Test
    public void testParseDropIndex() throws Exception {
        // Schema that is not set defaults to default schema of connection which is sch1
        assertDropIndexEquals(buildDropIndex("idx", "sch1", false), "drop index idx");
        assertDropIndexEquals(buildDropIndex("idx", "sch1", true), "drop index if exists idx");
        assertDropIndexEquals(buildDropIndex("idx", "sch1", true), "drop index if exists sch1.idx");
        assertDropIndexEquals(buildDropIndex("idx", "sch1", false), "drop index sch1.idx");

        // Message is null as long as it may differ from system to system, so we just check for exceptions
        assertParseThrows("drop index schema2.", DbException.class, null);
        assertParseThrows("drop index", DbException.class, null);
        assertParseThrows("drop index if exists", DbException.class, null);
        assertParseThrows("drop index if exists schema2.", DbException.class, null);
    }

    /**
     *
     */
    @Test
    public void testParseDropTable() throws Exception {
        // Schema that is not set defaults to default schema of connection which is sch1
        assertDropTableEquals(buildDropTable("sch1", "tbl", false), "drop table tbl");
        assertDropTableEquals(buildDropTable("sch1", "tbl", true), "drop table if exists tbl");
        assertDropTableEquals(buildDropTable("sch1", "tbl", true), "drop table if exists sch1.tbl");
        assertDropTableEquals(buildDropTable("sch1", "tbl", false), "drop table sch1.tbl");

        // Message is null as long as it may differ from system to system, so we just check for exceptions
        assertParseThrows("drop table schema2.", DbException.class, null);
        assertParseThrows("drop table", DbException.class, null);
        assertParseThrows("drop table if exists", DbException.class, null);
        assertParseThrows("drop table if exists schema2.", DbException.class, null);
    }

    /** */
    @Test
    public void testParseCreateTable() throws Exception {
        assertCreateTableEquals(
            buildCreateTable("sch1", "Person", "cache", F.asList("id", "city"),
                true, c("id", Value.INT), c("city", Value.STRING), c("name", Value.STRING),
                c("surname", Value.STRING), c("age", Value.INT)),
            "CREATE TABLE IF NOT EXISTS sch1.\"Person\" (\"id\" integer, \"city\" varchar," +
                " \"name\" varchar, \"surname\" varchar, \"age\" integer, PRIMARY KEY (\"id\", \"city\")) WITH " +
                "\"template=cache\"");

        assertCreateTableEquals(
            buildCreateTable("sch1", "Person", "cache", F.asList("id"),
                false, c("id", Value.INT), c("city", Value.STRING), c("name", Value.STRING),
                c("surname", Value.STRING), cn("age", Value.INT)),
            "CREATE TABLE sch1.\"Person\" (\"id\" integer PRIMARY KEY, \"city\" varchar," +
                " \"name\" varchar, \"surname\" varchar, \"age\" integer NOT NULL) WITH " +
                "\"template=cache\"");

        assertParseThrows("create table Person (id int)",
            IgniteSQLException.class, "No PRIMARY KEY defined for CREATE TABLE");

        assertParseThrows("create table Person (id int) AS SELECT 2 * 2",
            IgniteSQLException.class, "CREATE TABLE ... AS ... syntax is not supported");

        assertParseThrows("create table Person (id int primary key)",
            IgniteSQLException.class, "Table must have at least one non PRIMARY KEY column.");

        assertParseThrows("create table Person (id int primary key, age int unique) WITH \"template=cache\"",
            IgniteSQLException.class, "Too many constraints - only PRIMARY KEY is supported for CREATE TABLE");

        assertParseThrows("create table Person (id int auto_increment primary key, age int) WITH \"template=cache\"",
            IgniteSQLException.class, "AUTO_INCREMENT columns are not supported");

        assertParseThrows("create table Person (id int primary key check id > 0, age int) WITH \"template=cache\"",
            IgniteSQLException.class, "Column CHECK constraints are not supported [colName=ID]");

        assertParseThrows("create table Person (id int as age * 2 primary key, age int) WITH \"template=cache\"",
            IgniteSQLException.class, "Computed columns are not supported [colName=ID]");

        assertParseThrows("create table Int (_key int primary key, _val int) WITH \"template=cache\"",
            IgniteSQLException.class, "Direct specification of _KEY and _VAL columns is forbidden");

        assertParseThrows("create table Person (" +
                "unquoted_id LONG, " +
                "\"quoted_id\" LONG, " +
                "PERSON_NAME VARCHAR(255), " +
                "PRIMARY KEY (UNQUOTED_ID, quoted_id)) " +
                "WITH \"template=cache\"",
            IgniteSQLException.class, "PRIMARY KEY column is not defined: QUOTED_ID");

        assertParseThrows("create table Person (" +
                "unquoted_id LONG, " +
                "\"quoted_id\" LONG, " +
                "PERSON_NAME VARCHAR(255), " +
                "PRIMARY KEY (\"unquoted_id\", \"quoted_id\")) " +
                "WITH \"template=cache\"",
            IgniteSQLException.class, "PRIMARY KEY column is not defined: unquoted_id");
    }

    /** */
    @Test
    public void testParseCreateTableWithDefaults() {
        assertParseThrows("create table Person (id int primary key, age int, " +
                "ts TIMESTAMP default CURRENT_TIMESTAMP()) WITH \"template=cache\"",
            IgniteSQLException.class, "Non-constant DEFAULT expressions are not supported [colName=TS]");

        assertParseThrows("create table Person (id int primary key, age int default 'test') " +
                "WITH \"template=cache\"",
            IgniteSQLException.class, "Invalid default value for column. " +
                "[colName=AGE, colType=INTEGER, dfltValueType=VARCHAR]");

        assertParseThrows("create table Person (id int primary key, name varchar default 1) " +
                "WITH \"template=cache\"",
            IgniteSQLException.class, "Invalid default value for column. " +
                "[colName=NAME, colType=VARCHAR, dfltValueType=INTEGER]");
    }

    /** */
    @Test
    public void testParseAlterTableAddColumn() throws Exception {
        assertAlterTableAddColumnEquals(buildAlterTableAddColumn("SCH2", "Person", false, false,
            c("COMPANY", Value.STRING)), "ALTER TABLE SCH2.Person ADD company varchar");

        assertAlterTableAddColumnEquals(buildAlterTableAddColumn("SCH2", "Person", true, true,
            c("COMPANY", Value.STRING)), "ALTER TABLE IF EXISTS SCH2.Person ADD if not exists company varchar");

        assertAlterTableAddColumnEquals(buildAlterTableAddColumn("SCH2", "Person", false, true,
            c("COMPANY", Value.STRING), c("city", Value.STRING)),
            "ALTER TABLE IF EXISTS SCH2.Person ADD (company varchar, \"city\" varchar)");

        assertAlterTableAddColumnEquals(buildAlterTableAddColumn("SCH2", "City", false, true,
            c("POPULATION", Value.INT)), "ALTER TABLE IF EXISTS SCH2.\"City\" ADD (population int)");

        assertAlterTableAddColumnEquals(buildAlterTableAddColumn("SCH2", "City", false, true,
            cn("POPULATION", Value.INT)), "ALTER TABLE IF EXISTS SCH2.\"City\" ADD (population int NOT NULL)");

        // There's no table with such name, but H2 parsing does not fail just yet.
        assertAlterTableAddColumnEquals(buildAlterTableAddColumn("SCH2", "City", false, false,
            c("POPULATION", Value.INT)), "ALTER TABLE SCH2.\"City\" ADD (population int)");

        assertAlterTableAddColumnEquals(buildAlterTableAddColumn("SCH2", "Person", true, false,
            c("NAME", Value.STRING)), "ALTER TABLE SCH2.Person ADD if not exists name varchar");

        // There's a column with such name, but H2 parsing does not fail just yet.
        assertAlterTableAddColumnEquals(buildAlterTableAddColumn("SCH2", "Person", false, false,
            c("NAME", Value.STRING)), "ALTER TABLE SCH2.Person ADD name varchar");

        // IF NOT EXISTS with multiple columns.
        assertParseThrows("ALTER TABLE IF EXISTS SCH2.Person ADD if not exists (company varchar, city varchar)",
            DbException.class, null);

        // Both BEFORE keyword.
        assertParseThrows("ALTER TABLE IF EXISTS SCH2.Person ADD if not exists company varchar before addrid",
            IgniteSQLException.class, "BEFORE keyword is not supported");

        // Both AFTER keyword.
        assertParseThrows("ALTER TABLE IF EXISTS SCH2.Person ADD if not exists company varchar after addrid",
            IgniteSQLException.class, "AFTER keyword is not supported");

        assertParseThrows("ALTER TABLE IF EXISTS SCH2.Person ADD if not exists company varchar first",
            IgniteSQLException.class, "FIRST keyword is not supported");

        // No such schema.
        assertParseThrows("ALTER TABLE SCH5.\"Person\" ADD (city varchar)", DbException.class, null);
    }

    /**
     * @param sql Statement.
     * @param exCls Exception class.
     * @param msg Expected message.
     */
    private void assertParseThrows(final String sql, Class<? extends Exception> exCls, String msg) {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                Prepared p = parse(sql);

                return new GridSqlQueryParser(false, log).parse(p);
            }
        }, exCls, msg);
    }

    /**
     * Parse SQL and compare it to expected instance.
     */
    private void assertCreateIndexEquals(GridSqlCreateIndex exp, String sql) throws Exception {
        Prepared prepared = parse(sql);

        GridSqlStatement stmt = new GridSqlQueryParser(false, log).parse(prepared);

        assertTrue(stmt instanceof GridSqlCreateIndex);

        assertCreateIndexEquals(exp, (GridSqlCreateIndex) stmt);
    }

    /**
     * Parse SQL and compare it to expected instance of DROP INDEX.
     */
    private void assertDropIndexEquals(GridSqlDropIndex exp, String sql) throws Exception {
        Prepared prepared = parse(sql);

        GridSqlStatement stmt = new GridSqlQueryParser(false, log).parse(prepared);

        assertTrue(stmt instanceof GridSqlDropIndex);

        assertDropIndexEquals(exp, (GridSqlDropIndex) stmt);
    }

    /**
     * Test two instances of {@link GridSqlDropIndex} for equality.
     */
    private static void assertDropIndexEquals(GridSqlDropIndex exp, GridSqlDropIndex actual) {
        assertEqualsIgnoreCase(exp.indexName(), actual.indexName());
        assertEqualsIgnoreCase(exp.schemaName(), actual.schemaName());
        assertEquals(exp.ifExists(), actual.ifExists());
    }

    /**
     *
     */
    private static GridSqlDropIndex buildDropIndex(String name, String schema, boolean ifExists) {
        GridSqlDropIndex res = new GridSqlDropIndex();

        res.indexName(name);
        res.schemaName(schema);
        res.ifExists(ifExists);

        return res;
    }

    /**
     * Parse SQL and compare it to expected instance of DROP TABLE.
     */
    private void assertCreateTableEquals(GridSqlCreateTable exp, String sql) throws Exception {
        Prepared prepared = parse(sql);

        GridSqlStatement stmt = new GridSqlQueryParser(false, log).parse(prepared);

        assertTrue(stmt instanceof GridSqlCreateTable);

        assertCreateTableEquals(exp, (GridSqlCreateTable) stmt);
    }

    /**
     * Test two instances of {@link GridSqlDropTable} for equality.
     */
    private static void assertCreateTableEquals(GridSqlCreateTable exp, GridSqlCreateTable actual) {
        assertEqualsIgnoreCase(exp.schemaName(), actual.schemaName());
        assertEqualsIgnoreCase(exp.tableName(), actual.tableName());
        assertEquals(exp.templateName(), actual.templateName());
        assertEquals(exp.primaryKeyColumns(), actual.primaryKeyColumns());
        assertEquals(new ArrayList<>(exp.columns().keySet()), new ArrayList<>(actual.columns().keySet()));

        for (Map.Entry<String, GridSqlColumn> col : exp.columns().entrySet()) {
            GridSqlColumn val = actual.columns().get(col.getKey());

            assertNotNull(val);

            assertEquals(col.getValue().columnName(), val.columnName());
            assertEquals(col.getValue().column().getType(), val.column().getType());
        }

        assertEquals(exp.ifNotExists(), actual.ifNotExists());
    }

    /**
     *
     */
    private static GridSqlCreateTable buildCreateTable(String schema, String tbl, String tplCacheName,
        Collection<String> pkColNames, boolean ifNotExists, GridSqlColumn... cols) {
        GridSqlCreateTable res = new GridSqlCreateTable();

        res.schemaName(schema);

        res.tableName(tbl);

        res.templateName(tplCacheName);

        res.primaryKeyColumns(new LinkedHashSet<>(pkColNames));

        LinkedHashMap<String, GridSqlColumn> m = new LinkedHashMap<>();

        for (GridSqlColumn col : cols)
            m.put(col.columnName(), col);

        res.columns(m);

        res.ifNotExists(ifNotExists);

        return res;
    }

    /**
     * Parse SQL and compare it to expected instance of ALTER TABLE.
     */
    private void assertAlterTableAddColumnEquals(GridSqlAlterTableAddColumn exp, String sql) throws Exception {
        Prepared prepared = parse(sql);

        GridSqlStatement stmt = new GridSqlQueryParser(false, log).parse(prepared);

        assertTrue(stmt instanceof GridSqlAlterTableAddColumn);

        assertAlterTableAddColumnEquals(exp, (GridSqlAlterTableAddColumn)stmt);
    }

    /** */
    private static GridSqlAlterTableAddColumn buildAlterTableAddColumn(String schema, String tbl,
        boolean ifNotExists, boolean ifTblExists, GridSqlColumn... cols) {
        GridSqlAlterTableAddColumn res = new GridSqlAlterTableAddColumn();

        res.schemaName(schema);

        res.tableName(tbl);

        res.ifNotExists(ifNotExists);

        res.ifTableExists(ifTblExists);

        res.columns(cols);

        return res;
    }

    /**
     * Test two instances of {@link GridSqlAlterTableAddColumn} for equality.
     */
    private static void assertAlterTableAddColumnEquals(GridSqlAlterTableAddColumn exp,
        GridSqlAlterTableAddColumn actual) {
        assertEqualsIgnoreCase(exp.schemaName(), actual.schemaName());
        assertEqualsIgnoreCase(exp.tableName(), actual.tableName());
        assertEquals(exp.columns().length, actual.columns().length);

        for (int i = 0; i < exp.columns().length; i++) {
            GridSqlColumn expCol = exp.columns()[i];
            GridSqlColumn col = actual.columns()[i];

            assertEquals(expCol.columnName(), col.columnName());
            assertEquals(expCol.column().getType(), col.column().getType());
        }

        assertEquals(exp.ifNotExists(), actual.ifNotExists());
        assertEquals(exp.ifTableExists(), actual.ifTableExists());
    }

    /**
     * @param name Column name.
     * @param type Column data type.
     * @return {@link GridSqlColumn} with given name and type.
     */
    private static GridSqlColumn c(String name, int type) {
        return new GridSqlColumn(new Column(name, type), null, name);
    }

    /**
     * Constructs non-nullable column.
     *
     * @param name Column name.
     * @param type Column data type.
     * @return {@link GridSqlColumn} with given name and type.
     */
    private static GridSqlColumn cn(String name, int type) {
        Column col = new Column(name, type);

        col.setNullable(false);

        return new GridSqlColumn(col, null, name);
    }

    /**
     * Parse SQL and compare it to expected instance of DROP TABLE.
     */
    private void assertDropTableEquals(GridSqlDropTable exp, String sql) throws Exception {
        Prepared prepared = parse(sql);

        GridSqlStatement stmt = new GridSqlQueryParser(false, log).parse(prepared);

        assertTrue(stmt instanceof GridSqlDropTable);

        assertDropTableEquals(exp, (GridSqlDropTable) stmt);
    }

    /**
     * Test two instances of {@link GridSqlDropTable} for equality.
     */
    private static void assertDropTableEquals(GridSqlDropTable exp, GridSqlDropTable actual) {
        assertEqualsIgnoreCase(exp.schemaName(), actual.schemaName());
        assertEqualsIgnoreCase(exp.tableName(), actual.tableName());
        assertEquals(exp.ifExists(), actual.ifExists());
    }

    /**
     *
     */
    private static GridSqlDropTable buildDropTable(String schema, String tbl, boolean ifExists) {
        GridSqlDropTable res = new GridSqlDropTable();

        res.schemaName(schema);
        res.tableName(tbl);
        res.ifExists(ifExists);

        return res;
    }

    /**
     * Test two instances of {@link GridSqlCreateIndex} for equality.
     */
    private static void assertCreateIndexEquals(GridSqlCreateIndex exp, GridSqlCreateIndex actual) {
        assertEquals(exp.ifNotExists(), actual.ifNotExists());
        assertEqualsIgnoreCase(exp.schemaName(), actual.schemaName());
        assertEqualsIgnoreCase(exp.tableName(), actual.tableName());

        assertEqualsIgnoreCase(exp.index().getName(), actual.index().getName());

        Iterator<Map.Entry<String, Boolean>> expFldsIt = exp.index().getFields().entrySet().iterator();
        Iterator<Map.Entry<String, Boolean>> actualFldsIt = actual.index().getFields().entrySet().iterator();

        while (expFldsIt.hasNext()) {
            assertTrue(actualFldsIt.hasNext());

            Map.Entry<String, Boolean> expEntry = expFldsIt.next();
            Map.Entry<String, Boolean> actualEntry = actualFldsIt.next();

            assertEqualsIgnoreCase(expEntry.getKey(), actualEntry.getKey());
            assertEquals(expEntry.getValue(), actualEntry.getValue());
        }

        assertFalse(actualFldsIt.hasNext());

        assertEquals(exp.index().getIndexType(), actual.index().getIndexType());
    }

    /**
     *
     */
    private static void assertEqualsIgnoreCase(String exp, String actual) {
        assertEquals((exp == null), (actual == null));

        if (exp != null)
            assertTrue(exp.equalsIgnoreCase(actual));
    }

    /**
     *
     */
    private static GridSqlCreateIndex buildCreateIndex(String name, String tblName, String schemaName,
        boolean ifNotExists, QueryIndexType type, int inlineSize, Object... flds) {
        QueryIndex idx = new QueryIndex();

        idx.setName(name);

        assert !F.isEmpty(flds) && flds.length % 2 == 0;

        LinkedHashMap<String, Boolean> trueFlds = new LinkedHashMap<>();

        for (int i = 0; i < flds.length / 2; i++)
            trueFlds.put((String)flds[i * 2], (Boolean)flds[i * 2 + 1]);

        idx.setFields(trueFlds);
        idx.setIndexType(type);
        idx.setInlineSize(inlineSize);

        GridSqlCreateIndex res = new GridSqlCreateIndex();

        res.schemaName(schemaName);
        res.tableName(tblName);
        res.ifNotExists(ifNotExists);
        res.index(idx);

        return res;
    }

    /**
     *
     */
    private H2PooledConnection connection() throws Exception {
        IgniteH2Indexing idx = (IgniteH2Indexing)((IgniteEx)ignite).context().query().getIndexing();

        return idx.connections().connection(idx.schema(DEFAULT_CACHE_NAME));
    }

    /**
     * @param sql Sql.
     */
    @SuppressWarnings("unchecked")
    private <T extends Prepared> T parse(String sql) throws Exception {
        try (H2PooledConnection conn = connection()) {
            Session ses = H2Utils.session(conn);

            H2Utils.setupConnection(conn,
                QueryContext.parseContext(null, true), false, false, false);

            return (T)ses.prepare(sql);
        }
    }

    /**
     * @param exp Sql 1.
     * @param actual Sql 2.
     */
    private void assertSqlEquals(String exp, String actual) {
        String nsql1 = normalizeSql(exp);
        String nsql2 = normalizeSql(actual);

        assertEquals(nsql1, nsql2);
    }

    /**
     * @param sql Sql.
     */
    private static String normalizeSql(String sql) {
        return sql.toLowerCase()
            .replaceAll("/\\*(?:.|\r|\n)*?\\*/", " ")
            .replaceAll("\\s*on\\s+1\\s*=\\s*1\\s*", " on true ")
            .replaceAll("\\s+", " ")
            .replaceAll("\\( +", "(")
            .replaceAll(" +\\)", ")")
            .trim();
    }

    /**
     * @param qry Query.
     */
    private void checkQuery(String qry) throws Exception {
        Prepared prepared = parse(qry);

        GridSqlStatement gQry = new GridSqlQueryParser(false, log).parse(prepared);

        String res = gQry.getSQL();

        System.out.println(normalizeSql(res));

        assertSqlEquals(U.firstNotNull(prepared.getPlanSQL(), prepared.getSQL()), res);
    }

    @QuerySqlFunction
    public static int cool1() {
        return 1;
    }

    @QuerySqlFunction
    public static ResultSet table0(Connection c, String a, int b) throws SQLException {
        return c.createStatement().executeQuery("select '" + a + "' as a, " + b + " as b");
    }

    /**
     *
     */
    public static class PersonKey implements Serializable {
        /** */
        @QuerySqlField
        @AffinityKeyMapped
        public int id;

        /** Should not be allowed in KEY clause of MERGE. */
        @QuerySqlField
        public String stuff;
    }

    /**
     *
     */
    public static class Person implements Serializable {
        @QuerySqlField(index = true)
        public Date date = new Date(System.currentTimeMillis());

        @QuerySqlField(index = true)
        public String name = "Ivan";

        @QuerySqlField(index = true)
        public String parentName;

        @QuerySqlField(index = true)
        public int addrId;

        @QuerySqlField
        public Integer[] addrIds;

        @QuerySqlField(index = true)
        public int old;
    }

    /**
     *
     */
    public static class Address implements Serializable {
        @QuerySqlField(index = true)
        public int id;

        @QuerySqlField(index = true)
        public int streetNumber;

        @QuerySqlField(index = true)
        public String street = "Nevskiy";
    }
}
