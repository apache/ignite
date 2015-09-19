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
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.command.Prepared;
import org.h2.command.dml.Query;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;

import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridQueryParsingTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static Ignite ignite;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        // Cache.
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(CacheMode.PARTITIONED);
        cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cc.setNearConfiguration(null);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setRebalanceMode(SYNC);
        cc.setSwapEnabled(false);
        cc.setSqlFunctionClasses(GridQueryParsingTest.class);
        cc.setIndexedTypes(
            String.class, Address.class,
            String.class, Person.class
        );

        c.setCacheConfiguration(cc);

        return c;
    }

    /** */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrid();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAllExamples() throws Exception {
        checkQuery("select ? limit ? offset ?");

        checkQuery("select cool1()");
        checkQuery("select cool1() z");

        checkQuery("select b,a from table0('aaa', 100)");
        checkQuery("select * from table0('aaa', 100)");
        checkQuery("select * from table0('aaa', 100) t0");
        checkQuery("select x.a, y.b from table0('aaa', 100) x natural join table0('bbb', 100) y");
        checkQuery("select * from table0('aaa', 100) x join table0('bbb', 100) y on x.a=y.a and x.b = 'bbb'");
        checkQuery("select * from table0('aaa', 100) x left join table0('bbb', 100) y on x.a=y.a and x.b = 'bbb'");
        checkQuery("select * from table0('aaa', 100) x left join table0('bbb', 100) y on x.a=y.a where x.b = 'bbb'");
        checkQuery("select * from table0('aaa', 100) x left join table0('bbb', 100) y where x.b = 'bbb'");

        checkQuery("select avg(old) from Person left join Address on Person.addrId = Address.id " +
            "where lower(Address.street) = lower(?)");

        checkQuery("select avg(old) from Person join Address on Person.addrId = Address.id " +
            "where lower(Address.street) = lower(?)");

        checkQuery("select avg(old) from Person left join Address where Person.addrId = Address.id " +
            "and lower(Address.street) = lower(?)");

        checkQuery("select avg(old) from Person, Address where Person.addrId = Address.id " +
            "and lower(Address.street) = lower(?)");

        checkQuery("select name, date from Person");
        checkQuery("select distinct name, date from Person");
        checkQuery("select * from Person p");
        checkQuery("select * from Person");
        checkQuery("select distinct * from Person");
        checkQuery("select p.name, date from Person p");

        checkQuery("select * from Person p, Address a");
        checkQuery("select * from Person, Address");
        checkQuery("select p.* from Person p, Address a");
        checkQuery("select person.* from Person, Address a");
        checkQuery("select p.*, street from Person p, Address a");
        checkQuery("select p.name, a.street from Person p, Address a");
        checkQuery("select distinct p.name, a.street from Person p, Address a");
        checkQuery("select distinct name, street from Person, Address group by old");
        checkQuery("select distinct name, street from Person, Address");
        checkQuery("select p1.name, a2.street from Person p1, Address a1, Person p2, Address a2");

        checkQuery("select p.name n, a.street s from Person p, Address a");
        checkQuery("select p.name, 1 as i, 'aaa' s from Person p");

        checkQuery("select p.name + 'a', 1 * 3 as i, 'aaa' s, -p.old, -p.old as old from Person p");
        checkQuery("select p.name || 'a' + p.name, (p.old * 3) % p.old - p.old / p.old, p.name = 'aaa', " +
            " p.name is p.name, p.old > 0, p.old >= 0, p.old < 0, p.old <= 0, p.old <> 0, p.old is not p.old, " +
            " p.old is null, p.old is not null " +
            " from Person p");

        checkQuery("select p.name from Person p where name <> 'ivan'");
        checkQuery("select p.name from Person p where name like 'i%'");
        checkQuery("select p.name from Person p where name regexp 'i%'");
        checkQuery("select p.name from Person p, Address a where p.name <> 'ivan' and a.id > 10 or not (a.id = 100)");

        checkQuery("select case p.name when 'a' then 1 when 'a' then 2 end as a from Person p");
        checkQuery("select case p.name when 'a' then 1 when 'a' then 2 else -1 end as a from Person p");

        checkQuery("select abs(p.old)  from Person p");
        checkQuery("select cast(p.old as numeric(10, 2)) from Person p");
        checkQuery("select cast(p.old as numeric(10, 2)) z from Person p");
        checkQuery("select cast(p.old as numeric(10, 2)) as z from Person p");

        checkQuery("select * from Person p where p.name in ('a', 'b', '_' + RAND())"); // test ConditionIn
        checkQuery("select * from Person p where p.name in ('a', 'b', 'c')"); // test ConditionInConstantSet
        checkQuery("select * from Person p where p.name in (select a.street from Address a)"); // test ConditionInConstantSet

        checkQuery("select (select a.street from Address a where a.id = p.addrId) from Person p"); // test ConditionInConstantSet

        checkQuery("select p.name, ? from Person p where name regexp ? and p.old < ?");

        checkQuery("select count(*) as a from Person");
        checkQuery("select count(*) as a, count(p.*), count(p.name) from Person p");
        checkQuery("select count(distinct p.name) from Person p");
        checkQuery("select name, count(*) cnt from Person group by name order by cnt desc limit 10");

        checkQuery("select p.name, avg(p.old), max(p.old) from Person p group by p.name");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by n");

        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.addrId, p.name");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name, p.addrId");
        checkQuery("select p.name n, max(p.old) + min(p.old) / count(distinct p.old) from Person p group by p.name");
        checkQuery("select p.name n, max(p.old) maxOld, min(p.old) minOld from Person p group by p.name having maxOld > 10 and min(p.old) < 1");

        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name order by n");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name order by p.name");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name order by p.name, m");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name order by p.name, max(p.old) desc");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name order by p.name nulls first");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name order by p.name nulls last");
        checkQuery("select p.name n from Person p order by p.old + 10");
        checkQuery("select p.name n from Person p order by p.old + 10, p.name");
        checkQuery("select p.name n from Person p order by p.old + 10, p.name desc");

        checkQuery("select p.name n from Person p, (select a.street from Address a where a.street is not null) ");
        checkQuery("select street from Person p, (select a.street from Address a where a.street is not null) ");
        checkQuery("select addr.street from Person p, (select a.street from Address a where a.street is not null) addr");

        checkQuery("select p.name n from \"\".Person p order by p.old + 10");

        checkQuery("select case when p.name is null then 'Vasya' end x from \"\".Person p");
        checkQuery("select case when p.name like 'V%' then 'Vasya' else 'Other' end x from \"\".Person p");
        checkQuery("select case when upper(p.name) = 'VASYA' then 'Vasya' when p.name is not null then p.name else 'Other' end x from \"\".Person p");

        checkQuery("select case p.name when 'Vasya' then 1 end z from \"\".Person p");
        checkQuery("select case p.name when 'Vasya' then 1 when 'Petya' then 2 end z from \"\".Person p");
        checkQuery("select case p.name when 'Vasya' then 1 when 'Petya' then 2 else 3 end z from \"\".Person p");
        checkQuery("select case p.name when 'Vasya' then 1 else 3 end z from \"\".Person p");

        checkQuery("select count(*) as a from Person union select count(*) as a from Address");
        checkQuery("select old, count(*) as a from Person group by old union select 1, count(*) as a from Address");
        checkQuery("select name from Person MINUS select street from Address");
        checkQuery("select name from Person EXCEPT select street from Address");
        checkQuery("select name from Person INTERSECT select street from Address");
        checkQuery("select name from Person UNION select street from Address limit 5");
        checkQuery("select name from Person UNION select street from Address limit ?");
        checkQuery("select name from Person UNION select street from Address limit ? offset ?");
        checkQuery("(select name from Person limit 4) UNION (select street from Address limit 1) limit ? offset ?");
        checkQuery("(select 2 a) union all (select 1) order by 1");
        checkQuery("(select 2 a) union all (select 1) order by a desc nulls first limit ? offset ?");
    }

    /**
     *
     */
    public void testExample1() throws Exception {
        Query select = parse("select p.name n, max(p.old) maxOld, min(p.old) minOld from Person p group by p.name having maxOld > 10 and min(p.old) < 1");

        GridSqlQueryParser ses = new GridSqlQueryParser();

        GridSqlQuery gridSelect = ses.parse(select);

        //System.out.println(select.getPlanSQL());
        System.out.println(gridSelect.getSQL());
    }

    /**
     *
     */
    private JdbcConnection connection() throws Exception {
        GridKernalContext ctx = ((IgniteEx)ignite).context();

        GridQueryProcessor qryProcessor = ctx.query();

        IgniteH2Indexing idx = U.field(qryProcessor, "idx");

        return (JdbcConnection)idx.connectionForSpace(null);
    }

    /**
     * @param sql Sql.
     */
    private <T extends Prepared> T parse(String sql) throws Exception {
        Session ses = (Session)connection().getSession();

        return (T)ses.prepare(sql);
    }

    /**
     * @param sql1 Sql 1.
     * @param sql2 Sql 2.
     */
    private void assertSqlEquals(String sql1, String sql2) {
        assertEquals(normalizeSql(sql1), normalizeSql(sql2));
    }

    /**
     * @param sql Sql.
     */
    private static String normalizeSql(String sql) {
        return sql.toLowerCase()
            .replaceAll("/\\*(?:.|\r|\n)*?\\*/", " ")
            .replaceAll("\\s*on\\s+1\\s*=\\s*1\\s*", " ")
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

        GridSqlQueryParser ses = new GridSqlQueryParser();

        String res;

        if (prepared instanceof Query)
            res = ses.parse((Query) prepared).getSQL();
        else
            throw new UnsupportedOperationException();

        System.out.println(normalizeSql(res));

        assertSqlEquals(prepared.getPlanSQL(), res);
    }

    @QuerySqlFunction
    public static int cool1() {
        return 1;
    }

    @QuerySqlFunction
    public static ResultSet table0(Connection c, String a, int b) throws SQLException {
        return c.createStatement().executeQuery("select '" + a + "' as a, " +  b + " as b");
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