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

package org.apache.ignite.internal.processors.cache;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 */
public class IgniteCacheDistributedJoinTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static Connection conn;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = ((TcpDiscoverySpi)cfg.getDiscoverySpi());

        spi.setIpFinder(IP_FINDER);

        CacheConfiguration<Integer, A> ccfga = new CacheConfiguration<>();

        ccfga.setName("a");
        ccfga.setSqlSchema("A");
        ccfga.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfga.setBackups(1);
        ccfga.setCacheMode(CacheMode.PARTITIONED);
        ccfga.setIndexedTypes(Integer.class, A.class);

        CacheConfiguration<Integer, B> ccfgb = new CacheConfiguration<>();

        ccfgb.setName("b");
        ccfgb.setSqlSchema("B");
        ccfgb.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfgb.setBackups(1);
        ccfgb.setCacheMode(CacheMode.PARTITIONED);
        ccfgb.setIndexedTypes(Integer.class, B.class);

        CacheConfiguration<Integer, C> ccfgc = new CacheConfiguration<>();

        ccfgc.setName("c");
        ccfgc.setSqlSchema("C");
        ccfgc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfgc.setBackups(1);
        ccfgc.setCacheMode(CacheMode.PARTITIONED);
        ccfgc.setIndexedTypes(Integer.class, C.class);

        cfg.setCacheConfiguration(ccfga, ccfgb, ccfgc);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(4);

        awaitPartitionMapExchange();

        conn = DriverManager.getConnection("jdbc:h2:mem:");

        Statement s = conn.createStatement();

        s.execute("create schema a");
        s.execute("create schema b");
        s.execute("create schema c");

        s.execute("create table a.a(a bigint, b bigint, c bigint)");
        s.execute("create table b.b(a bigint, b bigint, c bigint)");
        s.execute("create table c.c(a bigint, b bigint, c bigint)");

        s.execute("create index on a.a(a)");
        s.execute("create index on a.a(b)");
        s.execute("create index on a.a(c)");

        s.execute("create index on b.b(a)");
        s.execute("create index on b.b(b)");
        s.execute("create index on b.b(c)");

        s.execute("create index on c.c(a)");
        s.execute("create index on c.c(b)");
        s.execute("create index on c.c(c)");

        GridRandom rnd = new GridRandom();
        Ignite ignite = ignite(0);

        IgniteCache<Integer,A> a = ignite.cache("a");
        IgniteCache<Integer,B> b = ignite.cache("b");
        IgniteCache<Integer,C> c = ignite.cache("c");

        for (int i = 0; i < 100; i++) {
            a.put(i, insert(s, new A(rnd.nextInt(50), rnd.nextInt(100), rnd.nextInt(150))));
            b.put(i, insert(s, new B(rnd.nextInt(100), rnd.nextInt(50), rnd.nextInt(150))));
            c.put(i, insert(s, new C(rnd.nextInt(150), rnd.nextInt(100), rnd.nextInt(50))));
        }

        checkSameResult(s, a, "select a, count(*) from a group by a order by a");
        checkSameResult(s, a, "select b, count(*) from a group by b order by b");
        checkSameResult(s, a, "select c, count(*) from a group by c order by c");

        checkSameResult(s, b, "select a, count(*) from b group by a order by a");
        checkSameResult(s, b, "select b, count(*) from b group by b order by b");
        checkSameResult(s, b, "select c, count(*) from b group by c order by c");

        checkSameResult(s, c, "select a, count(*) from c group by a order by a");
        checkSameResult(s, c, "select b, count(*) from c group by b order by b");
        checkSameResult(s, c, "select c, count(*) from c group by c order by c");

        s.close();
    }

    /**
     * @param s Statement.
     * @param c Cache.
     * @param qry Query.
     * @throws SQLException If failed.
     */
    private <Z extends X> void checkSameResult(Statement s, IgniteCache<Integer, Z> c, String qry) throws SQLException {
        s.executeUpdate("SET SCHEMA " + c.getName());

        try (
            ResultSet rs1 = s.executeQuery(qry);
            QueryCursor<List<?>> rs2 = c.query(new SqlFieldsQuery(qry).setDistributedJoins(true))
        ) {
            Iterator<List<?>> iter = rs2.iterator();

            for (;;) {
                if (!rs1.next()) {
                    assertFalse(iter.hasNext());

                    return;
                }

                assertTrue(iter.hasNext());

                List<?> row = iter.next();

                for (int i = 0; i < row.size(); i++)
                    assertEquals(rs1.getLong(i + 1), row.get(i));
            }
        }
    }

    /**
     * @param s Statement.
     * @param z Value.
     * @return Value.
     * @throws SQLException If failed.
     */
    private static <Z extends X> Z insert(Statement s, Z z) throws SQLException {
        String tbl = z.getClass().getSimpleName();

        tbl = tbl + "." + tbl;

        String insert = "insert into " + tbl + " values(" + z.a + ", " + z.b + ", " + z.c + ")";

        s.executeUpdate(insert);

        return z;
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoins() throws Exception {
        Ignite ignite = ignite(0);

        IgniteCache<Integer,A> a = ignite.cache("a");
        IgniteCache<Integer,B> b = ignite.cache("b");
        IgniteCache<Integer,C> c = ignite.cache("c");

        Statement s = conn.createStatement();

        checkSameResult(s, a, "select a.c, b.b, c.a from a.a, b.b, c.c where a.a = b.a and b.c = c.c order by a.c, b.b, c.a");
        checkSameResult(s, b, "select a.a, b.c, c.b from a.a, b.b, c.c where a.b = b.b and b.a = c.a order by a.a, b.c, c.b");
        checkSameResult(s, c, "select a.b, b.a, c.c from a.a, b.b, c.c where a.c = b.c and b.b = c.b order by a.b, b.a, c.c");

        for (int i = 0; i < 150; i++) {
            checkSameResult(s, a, "select a.c, b.b, c.a from a.a, b.b, c.c where " +
                i + " = a.c and a.a = b.a and b.c = c.c order by a.c, b.b, c.a");
            checkSameResult(s, b, "select a.a, b.c, c.b from a.a, b.b, c.c where " +
                i + " = c.b and a.b = b.b and b.a = c.a order by a.a, b.c, c.b");
            checkSameResult(s, c, "select a.b, b.a, c.c from a.a, b.b, c.c where " +
                i + " = b.c and a.c = b.c and b.b = c.b order by a.b, b.a, c.c");
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        U.closeQuiet(conn);
        stopAllGrids();
    }

    /**
     *
     */
    public static class X {
        /** */
        public long a;

        /** */
        public long b;

        /** */
        public long c;

        /**
         * @param a A.
         * @param b B.
         * @param c C.
         */
        public X(int a, int b, int c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        /** */
        @QuerySqlField(index = true)
        public long getA() {
            return a;
        }

        /** */
        @QuerySqlField(index = true)
        public long getB() {
            return b;
        }

        /** */
        @QuerySqlField(index = true)
        public long getC() {
            return c;
        }
    }

    /**
     *
     */
    public static class A extends X {
        /**
         * @param a A.
         * @param b B.
         * @param c C.
         */
        public A(int a, int b, int c) {
            super(a, b, c);
        }
    }

    /**
     *
     */
    public static class B extends X {
        /**
         * @param a A.
         * @param b B.
         * @param c C.
         */
        public B(int a, int b, int c) {
            super(a, b, c);
        }
    }

    /**
     *
     */
    public static class C extends X {
        /**
         * @param a A.
         * @param b B.
         * @param c C.
         */
        public C(int a, int b, int c) {
            super(a, b, c);
        }
    }
}
