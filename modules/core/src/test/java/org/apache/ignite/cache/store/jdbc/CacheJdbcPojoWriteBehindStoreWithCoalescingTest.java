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

package org.apache.ignite.cache.store.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.store.jdbc.dialect.H2Dialect;
import org.apache.ignite.cache.store.jdbc.model.TestJdbcPojoDataSourceFactory;
import org.apache.ignite.cache.store.jdbc.model.TestJdbcPojoStoreFactoryWithHangWriteAll;
import org.apache.ignite.cache.store.jdbc.model.TestPojo;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE;
import static org.apache.ignite.configuration.DataPageEvictionMode.RANDOM_LRU;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;

/**
 * Tests for {@link CacheJdbcPojoStore}.
 */
public class CacheJdbcPojoWriteBehindStoreWithCoalescingTest extends GridCommonAbstractTest {
    /** */
    private static final String DFLT_CONN_URL = "jdbc:h2:mem:TestDatabase;DB_CLOSE_DELAY=-1";

    /** */
    private boolean isHangOnWriteAll = false;

    /** */
    private boolean isSmallRegion = false;

    /** */
    private final AtomicBoolean testFailed = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 20 * 60 * 1000; //20 min
    }

    /** */
    public DataStorageConfiguration getDataStorageConfiguration() {
        DataStorageConfiguration memCfg = new DataStorageConfiguration();

        DataRegionConfiguration plc = new DataRegionConfiguration();

        plc.setName("Default_Region");

        plc.setPageEvictionMode(RANDOM_LRU);

        if (isSmallRegion)
            plc.setMaxSize(128L * 1024 * 1024); // 128 MB
        else
            plc.setMaxSize(1L * 1024 * 1024 * 1024); // 1GB

        memCfg.setDefaultDataRegionConfiguration(plc);

        memCfg.setWalMode(LOG_ONLY);

        return memCfg;
    }

    /** */
    public TestJdbcPojoDataSourceFactory getDataSourceFactory() {
        TestJdbcPojoDataSourceFactory testJdbcPojoDataSourceFactory = new TestJdbcPojoDataSourceFactory();

        testJdbcPojoDataSourceFactory.setURL("jdbc:h2:mem:TestDatabase;DB_CLOSE_DELAY=-1");

        testJdbcPojoDataSourceFactory.setUserName("sa");

        testJdbcPojoDataSourceFactory.setPassword("");

        return testJdbcPojoDataSourceFactory;
    }

    /** */
    public JdbcType getJdbcType() {
        JdbcType type = new JdbcType();

        type.setCacheName("TEST_CACHE");

        type.setKeyType(Integer.class);

        type.setValueType(TestPojo.class);

        type.setDatabaseSchema("PUBLIC");

        type.setDatabaseTable("TEST_CACHE");

        type.setKeyFields(new JdbcTypeField(java.sql.Types.INTEGER, "VALUE2", Integer.class, "value2"));

        type.setValueFields(
            new JdbcTypeField(java.sql.Types.VARCHAR, "VALUE1", String.class, "value1"),
            new JdbcTypeField(java.sql.Types.DATE, "VALUE3", java.sql.Date.class, "value3")
        );

        return type;
    }

    /** */
    public CacheJdbcPojoStoreFactory getStoreFactory() {
        CacheJdbcPojoStoreFactory storeFactory = new CacheJdbcPojoStoreFactory();

        storeFactory.setParallelLoadCacheMinimumThreshold(100);

        storeFactory.setBatchSize(100);

        storeFactory.setMaximumPoolSize(4);

        storeFactory.setDataSourceFactory(getDataSourceFactory());

        storeFactory.setDialect(new H2Dialect());

        storeFactory.setTypes(getJdbcType());

        return storeFactory;
    }

    /** */
    public CacheJdbcPojoStoreFactory getStoreFactoryWithHangWriteAll() {
        TestJdbcPojoStoreFactoryWithHangWriteAll storeFactory = new TestJdbcPojoStoreFactoryWithHangWriteAll();

        storeFactory.setParallelLoadCacheMinimumThreshold(100);

        storeFactory.setBatchSize(100);

        storeFactory.setMaximumPoolSize(4);

        storeFactory.setDataSourceFactory(getDataSourceFactory());

        storeFactory.setDialect(new H2Dialect());

        storeFactory.setTypes(getJdbcType());

        return storeFactory;
    }

    /** */
    public CacheConfiguration getCacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName("TEST_CACHE");

        ccfg.setCacheMode(REPLICATED);

        ccfg.setAtomicityMode(ATOMIC);

        ccfg.setPartitionLossPolicy(READ_WRITE_SAFE);

        ccfg.setReadThrough(true);

        ccfg.setWriteThrough(true);

        ccfg.setWriteBehindEnabled(true);

        ccfg.setWriteBehindBatchSize(1000);

        QueryEntity queryEntity = new QueryEntity();

        queryEntity.setKeyType("java.lang.Integer");

        queryEntity.setValueType("org.apache.ignite.cache.store.jdbc.model.TestPojo");

        queryEntity.setTableName("TEST_CACHE");

        queryEntity.setKeyFieldName("value3");

        Set<String> keyFiles = new HashSet<>();

        keyFiles.add("value3");

        queryEntity.setKeyFields(keyFiles);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("value1", "java.lang.String");

        fields.put("value2", "java.lang.Integer");

        fields.put("value3", "java.sql.Date");

        queryEntity.setFields(fields);

        Map<String, String> aliases = new HashMap<>();

        aliases.put("value1", "VALUE1");

        aliases.put("value2", "VALUE2");

        aliases.put("value3", "VALUE3");

        queryEntity.setAliases(aliases);

        ArrayList<QueryEntity> queryEntities = new ArrayList<>();

        queryEntities.add(queryEntity);

        ccfg.setQueryEntities(queryEntities);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        //Data Storage

        cfg.setDataStorageConfiguration(getDataStorageConfiguration());

        //cache configuration

        CacheConfiguration ccfg = getCacheConfiguration();

        if (isHangOnWriteAll)
            ccfg.setCacheStoreFactory(getStoreFactoryWithHangWriteAll());
        else
            ccfg.setCacheStoreFactory(getStoreFactory());

        cfg.setCacheConfiguration(ccfg);

        //discovery

        TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder tcpDiscoveryVmIpFinder = new TcpDiscoveryVmIpFinder();

        ArrayList<String> addrs = new ArrayList<>();

        addrs.add("127.0.0.1:47500..47509");

        tcpDiscoveryVmIpFinder.setAddresses(addrs);

        tcpDiscoverySpi.setIpFinder(tcpDiscoveryVmIpFinder);

        cfg.setDiscoverySpi(tcpDiscoverySpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        testFailed.set(false);

        cleanPersistenceDir();

        try {
            Connection conn = DriverManager.getConnection(DFLT_CONN_URL, "sa", "");

            Statement stmt = conn.createStatement();

            stmt.executeUpdate("DROP TABLE IF EXISTS TEST_CACHE");

            stmt.executeUpdate("CREATE TABLE TEST_CACHE (" +
                " VALUE2 INTEGER PRIMARY KEY," +
                " VALUE1 VARCHAR(50)," +
                " VALUE3 DATE" +
                ")"
            );

            conn.commit();

            U.closeQuiet(stmt);

            U.closeQuiet(conn);
        } catch (SQLException ex) {
            fail(ex.getMessage());
        }

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    public void checkCacheStore(IgniteCache<Integer, TestPojo> cache) {
        try {
            Connection conn = DriverManager.getConnection(DFLT_CONN_URL, "sa", "");

            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(" SELECT * FROM TEST_CACHE");

            int count = 0;

            while (rs.next()) {
                String value1 = rs.getString("VALUE1");

                Integer value2 = rs.getInt("VALUE2");

                java.sql.Date value3 = rs.getDate("VALUE3");

                TestPojo pojo = cache.get(value2);

                assertNotNull(pojo);

                Calendar c1 = Calendar.getInstance();

                c1.setTime(value3);

                Calendar c2 = Calendar.getInstance();

                c2.setTime(pojo.getValue3());

                assertEquals(value1, pojo.getValue1());

                assertEquals(value2, pojo.getValue2());

                assertEquals(c1.get(Calendar.DAY_OF_YEAR), c2.get(Calendar.DAY_OF_YEAR));

                assertEquals(c1.get(Calendar.YEAR), c2.get(Calendar.YEAR));

                assertEquals(c1.get(Calendar.MONTH), c2.get(Calendar.MONTH));

                count++;
            }

            assertEquals(count, cache.size());

            U.closeQuiet(stmt);

            U.closeQuiet(conn);
        } catch (SQLException ex) {
            fail();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHangWriteAllWithCoalescing() throws Exception {
        isHangOnWriteAll = true;

        writeAllWithCoalescing();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNormalWriteAllWithCoalescing() throws Exception {
        isHangOnWriteAll = false;

        writeAllWithCoalescing();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadWithCoalescingSmallRegionWithEviction() throws Exception {
        isHangOnWriteAll = false;

        isSmallRegion = true;

        readWithCoalescing();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadWithCoalescingNormalRegion() throws Exception {
        isHangOnWriteAll = false;

        isSmallRegion = false;

        readWithCoalescing();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateAndReadTheSameKeyWithCoalescing() throws Exception {
        isHangOnWriteAll = false;

        isSmallRegion = false;

        updateAndReadWithCoalescingSameKey();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateAndReadTheSameKeyWithCoalescingHangWriteAll() throws Exception {
        isHangOnWriteAll = true;

        isSmallRegion = false;

        updateAndReadWithCoalescingSameKey();
    }

    /**
     * @throws Exception If failed.
     */
    public void updateAndReadWithCoalescingSameKey() throws Exception {
        Ignite ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Integer, TestPojo> cache = grid(0).cache("TEST_CACHE");

        AtomicInteger t1Count = new AtomicInteger(5);

        AtomicInteger t2Count = new AtomicInteger(5);

        Thread t1 = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    while (t1Count.get() > 0) {
                        for (int i = 0; i < 200000; i++) {
                            TestPojo next = new TestPojo("ORIGIN" + i, i, new java.sql.Date(new java.util.Date().getTime()));

                            cache.put(1, next);

                            TestPojo ret = cache.get(1);

                            assertEquals(ret, next);
                        }

                        t1Count.decrementAndGet();
                    }
                } catch (CacheException e) {
                    //ignore
                }
            }
        });

        Thread t2 = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    while (t2Count.get() > 0) {
                        for (int i = 200000; i < 400000; i++) {
                            TestPojo next = new TestPojo("ORIGIN" + i, i, new java.sql.Date(new java.util.Date().getTime()));

                            cache.put(2, next);

                            TestPojo ret = cache.get(2);

                            assertEquals(ret, next);
                        }

                        t2Count.decrementAndGet();
                    }
                } catch (CacheException e) {
                    //ignore
                }
            }
        });

        TestErrorHandler handler = new TestErrorHandler();

        t1.setUncaughtExceptionHandler(handler);

        t2.setUncaughtExceptionHandler(handler);

        t1.start();

        t2.start();

        t1.join();

        t2.join();

        assertFalse(testFailed.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void readWithCoalescing() throws Exception {
        Ignite ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Integer, TestPojo> cache = grid(0).cache("TEST_CACHE");

        AtomicInteger t1Count = new AtomicInteger(5);

        AtomicInteger t2Count = new AtomicInteger(5);

        Thread t1 = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    while (t1Count.get() > 0) {
                        for (int i = 0; i < 200000; i++) {
                            TestPojo next = new TestPojo("ORIGIN" + i, i, new java.sql.Date(new java.util.Date().getTime()));

                            cache.put(i, next);

                            TestPojo ret = cache.get(i);

                            assertEquals(ret, next);
                        }

                        t1Count.decrementAndGet();
                    }
                } catch (CacheException e) {
                    //ignore
                }
            }
        });

        Thread t2 = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    while (t2Count.get() > 0) {
                        for (int i = 200000; i < 400000; i++) {
                            TestPojo next = new TestPojo("ORIGIN" + i, i, new java.sql.Date(new java.util.Date().getTime()));

                            cache.put(i, next);

                            TestPojo ret = cache.get(i);

                            assertEquals(ret, next);
                        }

                        t2Count.decrementAndGet();
                    }
                } catch (CacheException e) {
                    //ignore
                }
            }
        });

        TestErrorHandler handler = new TestErrorHandler();

        t1.setUncaughtExceptionHandler(handler);

        t2.setUncaughtExceptionHandler(handler);

        t1.start();

        t2.start();

        t1.join();

        t2.join();

        assertFalse(testFailed.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void writeAllWithCoalescing() throws Exception {
        Ignite ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Integer, TestPojo> cache = grid(0).cache("TEST_CACHE");

        AtomicInteger t1Count = new AtomicInteger(10);

        AtomicInteger t2Count = new AtomicInteger(10);

        Thread t1 = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    while (t1Count.get() > 0) {
                        for (int i = 0; i < 5000; i++)
                            cache.put(i, new TestPojo("ORIGIN" + i, i, new java.sql.Date(new java.util.Date().getTime())));

                        t1Count.decrementAndGet();
                    }
                } catch (CacheException e) {
                    //ignore
                }
            }
        });

        Thread t2 = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    while (t2Count.get() > 0) {
                        for (int i = 0; i < 5000; i++)
                            cache.put(i, new TestPojo("UPDATE" + i, i, new java.sql.Date(new java.util.Date().getTime())));

                        try {
                            U.sleep(500);
                        }
                        catch (IgniteInterruptedCheckedException e) {
                            e.printStackTrace();
                        }

                        t2Count.decrementAndGet();
                    }
                } catch (CacheException e) {
                    //ignore
                }
            }
        });

        t1.start();

        t2.start();

        //t1 should be completed before 10 seconds.
        U.sleep(10_000);

        assertEquals(0, t1Count.get());

        t1.join();

        t2.join();

        assertEquals(0, t2Count.get());

        //now wait for updates will be done on store size and check that the data set is the same
        if (isHangOnWriteAll)
            //max time -> 10000 updates that will be send by 1000 batches -> (10000 / 1000) * 10 = 100 seconds.
            U.sleep(100_000);
        else
            U.sleep(10_000);

        checkCacheStore(cache);
    }

    /**
     * @throws Exception If failed.
     */
    private class TestErrorHandler implements Thread.UncaughtExceptionHandler {
        /** {@inheritDoc} */
        @Override public void uncaughtException(Thread t, Throwable e) {
            testFailed.set(true);
        }
    }
}

