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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.processors.query.h2.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.h2.command.*;
import org.h2.command.dml.*;
import org.h2.engine.*;
import org.h2.jdbc.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

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

        QueryConfiguration idxCfg = new QueryConfiguration();

        c.setQueryConfiguration(idxCfg);

        c.setMarshaller(new OptimizedMarshaller(true));

        // Cache.
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(CacheMode.PARTITIONED);
        cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cc.setDistributionMode(PARTITIONED_ONLY);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setRebalanceMode(SYNC);
        cc.setSwapEnabled(false);

        CacheQueryConfiguration qcfg = new CacheQueryConfiguration();

        qcfg.setIndexPrimitiveKey(true);
        qcfg.setIndexFixedTyping(true);

        cc.setQueryConfiguration(qcfg);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrid();

        GridCache cache = ((IgniteKernal)ignite).cache(null);

        cache.putx("testAddr", new Address());
        cache.putx("testPerson", new Person());
    }

    /**
     *
     */
    public void testAllExampless() throws Exception {
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

        checkQuery("select p.name n from PUBLIC.Person p order by p.old + 10");
    }

    /**
     *
     */
    public void testExample1() throws Exception {
        Select select = parse("select p.name n, max(p.old) maxOld, min(p.old) minOld from Person p group by p.name having maxOld > 10 and min(p.old) < 1");

        GridSqlQueryParser ses = new GridSqlQueryParser();

        GridSqlSelect gridSelect = ses.parse(select);

        //System.out.println(select.getPlanSQL());
        System.out.println(gridSelect.getSQL());
    }

    /**
     *
     */
    private JdbcConnection connection() throws Exception {
        GridKernalContext ctx = ((IgniteKernal)ignite).context();

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

        if (prepared instanceof Select)
            res = ses.parse((Select) prepared).getSQL();
        else
            throw new UnsupportedOperationException();

        assertSqlEquals(prepared.getPlanSQL(), res);

        System.out.println(normalizeSql(res));
    }

    /**
     *
     */
    public static class Person implements Serializable {
        @QuerySqlField(index = true)
        public Date date = new Date();

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
