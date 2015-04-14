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
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.sql.*;
import java.util.*;

/**
 * Abstract test framework to compare query results from h2 database instance and mixed ignite caches (replicated and partitioned)
 * which have the same data models and data content. 
 */
public abstract class AbstractH2CompareQueryTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

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

        c.setCacheConfiguration(createCache("part", CacheMode.PARTITIONED),
            createCache("repl", CacheMode.REPLICATED)
        );

        return c;
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

        Ignite ignite = startGrids(4);

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
     */
    protected abstract void initCacheAndDbData() throws SQLException;

    /**
     * @throws Exception If failed.
     */
    protected abstract void checkAllDataEquals() throws Exception;

    /**
     * Initialize h2 database schema.
     *
     * @throws SQLException If exception.
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
    private Connection openH2Connection(boolean autocommit) throws SQLException {
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
    protected final List<List<?>> compareQueryRes0(IgniteCache cache, String sql, @Nullable Object[] args, Ordering ordering) throws SQLException {
        if (args == null)
            args = new Object[] {null};
        
        info("Sql query:\n" + sql + "\nargs=" + Arrays.toString(args));

        List<List<?>> h2Res = executeH2Query(sql, args);

        List<List<?>> cacheRes = cache.query(new SqlFieldsQuery(sql).setArgs(args)).getAll();

        assertRsEquals(h2Res, cacheRes, ordering);
        
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
    private List<List<?>> executeH2Query(String sql, Object[] args) throws SQLException {
        List<List<?>> res = new ArrayList<>();
        ResultSet rs = null;

        try(PreparedStatement st = conn.prepareStatement(sql)) {
            for (int idx = 0; idx < args.length; idx++)
                st.setObject(idx + 1, args[idx]);

            rs = st.executeQuery();

            int colCnt = rs.getMetaData().getColumnCount();

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
    private void assertRsEquals(List<List<?>> rs1, List<List<?>> rs2, Ordering ordering) {
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
                Map<List<?>, Integer> rowsWithCnt1 = extractUniqueRowsWithCounts(rs1);
                Map<List<?>, Integer> rowsWithCnt2 = extractUniqueRowsWithCounts(rs2);
                
                assertEquals("Unique rows count has to be equal.", rowsWithCnt1.size(), rowsWithCnt2.size());

                for (Map.Entry<List<?>, Integer> entry1 : rowsWithCnt1.entrySet()) {
                    List<?> row = entry1.getKey();
                    Integer cnt1 = entry1.getValue();

                    Integer cnt2 = rowsWithCnt2.get(row);

                    assertEquals("Row has different occurance number.\nRow=" + row, cnt1, cnt2);
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
    private Map<List<?>, Integer> extractUniqueRowsWithCounts(Iterable<List<?>> rs) {
        Map<List<?>, Integer> res = new HashMap<>();

        for (List<?> row : rs) {
            Integer cnt = res.get(row);
            
            if (cnt == null)
                cnt = 0;
            
            res.put(row, cnt + 1);
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
