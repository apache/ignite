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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import junit.framework.AssertionFailedError;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract test framework to compare query results from h2 database instance and mixed ignite caches (replicated and partitioned)
 * which have the same data models and data content.
 */
public abstract class AbstractH2CompareQueryTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    protected static final int SRVS = 4;

    /** Partitioned cache. */
    protected static IgniteCache pCache;

    /** Replicated cache. */
    protected static IgniteCache rCache;

    /** H2 db connection. */
    protected static Connection conn;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        c.setMarshaller(new OptimizedMarshaller(true));

        c.setCacheConfiguration(cacheConfigurations());

        return c;
    }

    /**
     * @return Cache configurations.
     */
    protected CacheConfiguration[] cacheConfigurations() {
        return new CacheConfiguration[] {
            createCache("part", CacheMode.PARTITIONED),
            createCache("repl", CacheMode.REPLICATED)};
    }

    /**
     * Creates new cache configuration.
     *
     * @param name Cache name.
     * @param mode Cache mode.
     * @return Cache configuration.
     */
    private CacheConfiguration createCache(String name, CacheMode mode) {
        CacheConfiguration<?,?> cc = defaultCacheConfiguration();

        cc.setName(name);
        cc.setCacheMode(mode);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        setIndexedTypes(cc, mode);

        return cc;
    }

    /**
     * @param cc Cache configuration.
     * @param mode Cache Mode.
     */
    protected abstract void setIndexedTypes(CacheConfiguration<?, ?> cc, CacheMode mode) ;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        Ignite ignite = startGrids(SRVS);

        pCache = ignite.cache("part");

        rCache = ignite.cache("repl");

        awaitPartitionMapExchange();

        conn = openH2Connection(false);

        initializeH2Schema();

        initCacheAndDbData();

        checkAllDataEquals();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        Statement st = conn.createStatement();

        st.execute("DROP ALL OBJECTS");

        conn.close();

        stopAllGrids();
    }

    /**
     * Populate cache and h2 database with test data.
     * @throws SQLException If failed.
     */
    protected abstract void initCacheAndDbData() throws Exception;

    /**
     * @throws Exception If failed.
     */
    protected abstract void checkAllDataEquals() throws Exception;

    /**
     * Initialize h2 database schema.
     *
     * @throws SQLException If exception.
     * @return Statement.
     */
    protected Statement initializeH2Schema() throws SQLException {
        Statement st = conn.createStatement();

        st.execute("CREATE SCHEMA \"part\"");
        st.execute("CREATE SCHEMA \"repl\"");

        return st;
    }

    /**
     * Gets connection from a pool.
     *
     * @param autocommit {@code true} If connection should use autocommit mode.
     * @return Pooled connection.
     * @throws SQLException In case of error.
     */
    protected Connection openH2Connection(boolean autocommit) throws SQLException {
        System.setProperty("h2.serializeJavaObject", "false");

        String dbName = "test";

        Connection conn = DriverManager.getConnection("jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1");

        conn.setAutoCommit(autocommit);

        return conn;
    }

    /**
     * Execute given sql query on h2 database and on partitioned ignite cache and compare results.
     *
     * @param sql SQL query.
     * @param args SQL arguments.
     * then results will compare as ordered queries.
     * @return Result set after SQL query execution.
     * @throws SQLException If exception.
     */
    protected final List<List<?>> compareQueryRes0(String sql, @Nullable Object... args) throws SQLException {
        return compareQueryRes0(pCache, sql, args, Ordering.RANDOM);
    }

    /**
     * Execute given sql query on h2 database and on ignite cache and compare results.
     * Expected that results are not ordered.
     *
     * @param cache Ignite cache.
     * @param sql SQL query.
     * @param args SQL arguments.
     * then results will compare as ordered queries.
     * @return Result set after SQL query execution.
     * @throws SQLException If exception.
     */
    protected final List<List<?>> compareQueryRes0(IgniteCache cache, String sql, @Nullable Object... args) throws SQLException {
        return compareQueryRes0(cache, sql, args, Ordering.RANDOM);
    }

    /**
     * Execute given sql query on h2 database and on partitioned ignite cache and compare results.
     * Expected that results are ordered.
     *
     * @param sql SQL query.
     * @param args SQL arguments.
     * then results will compare as ordered queries.
     * @return Result set after SQL query execution.
     * @throws SQLException If exception.
     */
    protected final List<List<?>> compareOrderedQueryRes0(String sql, @Nullable Object... args) throws SQLException {
        return compareQueryRes0(pCache, sql, args, Ordering.ORDERED);
    }

    /**
     * Execute given sql query on h2 database and on ignite cache and compare results.
     *
     * @param cache Ignite cache.
     * @param sql SQL query.
     * @param args SQL arguments.
     * @param ordering Expected ordering of SQL results. If {@link Ordering#ORDERED}
     * then results will compare as ordered queries.
     * @return Result set after SQL query execution.
     * @throws SQLException If exception.
     */
    @SuppressWarnings("unchecked")
    protected static List<List<?>> compareQueryRes0(IgniteCache cache, String sql, @Nullable Object[] args,
        Ordering ordering) throws SQLException {
        return compareQueryRes0(cache, sql, false, args, ordering);
    }

    /**
     * Execute given sql query on h2 database and on ignite cache and compare results.
     *
     * @param cache Ignite cache.
     * @param sql SQL query.
     * @param distrib Distributed SQL Join flag.
     * @param args SQL arguments.
     * @param ordering Expected ordering of SQL results. If {@link Ordering#ORDERED}
     *      then results will compare as ordered queries.
     * @return Result set after SQL query execution.
     * @throws SQLException If exception.
     */
    protected static List<List<?>> compareQueryRes0(IgniteCache cache,
        String sql,
        boolean distrib,
        @Nullable Object[] args,
        Ordering ordering) throws SQLException {
        return compareQueryRes0(cache, sql, distrib, false, args, ordering);
    }

    /**
     * Execute given sql query on h2 database and on ignite cache and compare results.
     *
     * @param cache Ignite cache.
     * @param sql SQL query.
     * @param distrib Distributed SQL Join flag.
     * @param enforceJoinOrder Enforce join order flag.
     * @param args SQL arguments.
     * @param ordering Expected ordering of SQL results. If {@link Ordering#ORDERED}
     *      then results will compare as ordered queries.
     * @return Result set after SQL query execution.
     * @throws SQLException If exception.
     */
    @SuppressWarnings("unchecked")
    protected static List<List<?>> compareQueryRes0(IgniteCache cache,
        String sql,
        boolean distrib,
        boolean enforceJoinOrder,
        @Nullable Object[] args,
        Ordering ordering) throws SQLException {
        if (args == null)
            args = new Object[] {null};

        List<List<?>> h2Res = executeH2Query(sql, args);

//        String plan = (String)((IgniteCache<Object, Object>)cache).query(new SqlFieldsQuery("explain " + sql)
//            .setArgs(args)
//            .setDistributedJoins(distrib))
//            .getAll().get(0).get(0);
//
//        X.println("Plan : " + plan);

        List<List<?>> cacheRes = cache.query(new SqlFieldsQuery(sql).
            setArgs(args).
            setDistributedJoins(distrib).
            setEnforceJoinOrder(enforceJoinOrder)).getAll();

        try {
            assertRsEquals(h2Res, cacheRes, ordering);
        }
        catch (AssertionFailedError e) {
            X.println("Sql query:\n" + sql + "\nargs=" + Arrays.toString(args));
            X.println("[h2Res=" + h2Res + ", cacheRes=" + cacheRes + "]");

            throw e;
        }

        return h2Res;
    }

    /**
     * Execute SQL query on h2 database.
     *
     * @param sql SQL query.
     * @param args SQL arguments.
     * @return Result of SQL query on h2 database.
     * @throws SQLException If exception.
     */
    private static List<List<?>> executeH2Query(String sql, Object[] args) throws SQLException {
        List<List<?>> res = new ArrayList<>();
        ResultSet rs = null;

        try (PreparedStatement st = conn.prepareStatement(sql)) {
            for (int idx = 0; idx < args.length; idx++)
                st.setObject(idx + 1, args[idx]);

            rs = st.executeQuery();

            ResultSetMetaData meta = rs.getMetaData();

            int colCnt = meta.getColumnCount();

//            for (int i = 1; i <= colCnt; i++)
//                X.print(meta.getColumnLabel(i) + "  ");
//
//            X.println();

            while (rs.next()) {
                List<Object> row = new ArrayList<>(colCnt);

                for (int i = 1; i <= colCnt; i++)
                    row.add(rs.getObject(i));

                res.add(row);
            }
        }
        finally {
            U.closeQuiet(rs);
        }

        return res;
    }

    /**
     * Assert equals of result sets according to expected ordering.
     *
     * @param rs1 Expected result set.
     * @param rs2 Actual result set.
     * @param ordering Expected ordering of SQL results. If {@link Ordering#ORDERED}
     * then results will compare as ordered queries.
     */
    private static void assertRsEquals(List<List<?>> rs1, List<List<?>> rs2, Ordering ordering) {
        assertEquals("Rows count has to be equal.", rs1.size(), rs2.size());

        switch (ordering){
            case ORDERED:
                for (int rowNum = 0; rowNum < rs1.size(); rowNum++) {
                    List<?> row1 = rs1.get(rowNum);
                    List<?> row2 = rs2.get(rowNum);

                    assertEquals("Columns count have to be equal.", row1.size(), row2.size());

                    for (int colNum = 0; colNum < row1.size(); colNum++)
                        assertEquals("Row=" + rowNum + ", column=" + colNum, row1.get(colNum), row2.get(colNum));
                }

                break;
            case RANDOM:
                TreeMap<String, Integer> rowsWithCnt1 = extractUniqueRowsWithCounts(rs1);
                TreeMap<String, Integer> rowsWithCnt2 = extractUniqueRowsWithCounts(rs2);

                assertEquals("Unique rows count has to be equal.", rowsWithCnt1.size(), rowsWithCnt2.size());

                // X.println("Result size: " + rowsWithCnt1.size());

                Iterator<Map.Entry<String,Integer>> iter1 = rowsWithCnt1.entrySet().iterator();
                Iterator<Map.Entry<String,Integer>> iter2 = rowsWithCnt2.entrySet().iterator();

                for (;;) {
                    if (!iter1.hasNext()) {
                        assertFalse(iter2.hasNext());

                        break;
                    }

                    assertTrue(iter2.hasNext());

                    Map.Entry<String, Integer> e1 = iter1.next();
                    Map.Entry<String, Integer> e2 = iter2.next();

                    assertEquals(e1.getKey(), e2.getKey());
                    assertEquals(e1.getValue(), e2.getValue());
                }

                break;
            default:
                throw new IllegalStateException();
        }
    }

    /**
     * @param rs Result set.
     * @return Map of unique rows at the result set to number of occuriances at the result set.
     */
    private static TreeMap<String, Integer> extractUniqueRowsWithCounts(Iterable<List<?>> rs) {
        TreeMap<String, Integer> res = new TreeMap<>();

        for (List<?> row : rs) {
            String rowStr = row.toString();

            Integer cnt = res.get(rowStr);

            if (cnt == null)
                cnt = 0;

            res.put(rowStr, cnt + 1);
        }

        return res;
    }

    /**
     * Ordering type.
     */
    protected enum Ordering {
        /** Random. */
        RANDOM,
        /** Ordered. */
        ORDERED
    }
}
