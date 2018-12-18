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

import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 */
@RunWith(JUnit4.class)
public class GridCacheQueryIndexDisabledSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    public GridCacheQueryIndexDisabledSelfTest() {
        super(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        cfg.setCacheConfiguration(ccfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSqlQuery() throws Exception {
        IgniteCache<Integer, SqlValue> cache = grid().getOrCreateCache(SqlValue.class.getSimpleName());

        try {
            cache.query(new SqlQuery<Integer, SqlValue>(SqlValue.class, "val >= 0")).getAll();

            assert false;
        }
        catch (CacheException e) {
            X.println("Caught expected exception: " + e);
        }
        catch (Exception ignored) {
            assert false;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSqlFieldsQuery() throws Exception {
        IgniteCache<Integer, SqlValue> cache = grid().getOrCreateCache(SqlValue.class.getSimpleName());

        try {
            cache.query(new SqlFieldsQuery("select * from Person")).getAll();

            assert false;
        }
        catch (CacheException e) {
            X.println("Caught expected exception: " + e);
        }
        catch (Exception ignored) {
            assert false;
        }

        try {
            cache.query(new SqlFieldsQuery("select * from Person")).getAll();

            assert false;
        }
        catch (CacheException e) {
            X.println("Caught expected exception: " + e);
        }
        catch (Exception ignored) {
            assert false;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFullTextQuery() throws Exception {
        IgniteCache<Integer, String> cache = grid().getOrCreateCache(String.class.getSimpleName());

        try {
            cache.query(new TextQuery<Integer, String>(String.class, "text")).getAll();

            assert false;
        }
        catch (CacheException e) {
            X.println("Caught expected exception: " + e);
        }
        catch (Exception ignored) {
            assert false;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanLocalQuery() throws Exception {
        IgniteCache<Integer, String> cache = grid().getOrCreateCache(String.class.getSimpleName());

        try {
            cache.query(new ScanQuery<>(new IgniteBiPredicate<Integer, String>() {
                @Override public boolean apply(Integer id, String s) {
                    return s.equals("");
                }
            }).setLocal(true)).getAll();
        }
        catch (IgniteException ignored) {
            assertTrue("Scan query should work with disable query indexing.", false);
        }
    }
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSqlLocalQuery() throws Exception {
        IgniteCache<Integer, SqlValue> cache = grid().getOrCreateCache(SqlValue.class.getSimpleName());

        try {
            cache.query(new SqlQuery<Integer, SqlValue>(SqlValue.class, "val >= 0").setLocal(true)).getAll();

            assert false;
        }
        catch (CacheException e) {
            X.println("Caught expected exception: " + e);
        }
        catch (Exception ignored) {
            assert false;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSqlLocalFieldsQuery() throws Exception {
        IgniteCache<Integer, SqlValue> cache = grid().getOrCreateCache(SqlValue.class.getSimpleName());

        try {
            cache.query(new SqlFieldsQuery("select * from Person")).getAll();

            assert false;
        }
        catch (CacheException e) {
            X.println("Caught expected exception: " + e);
        }
        catch (Exception ignored) {
            assert false;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFullTextLocalQuery() throws Exception {
        IgniteCache<Integer, String> cache = grid().getOrCreateCache(String.class.getSimpleName());

        try {
            cache.query(new TextQuery<Integer, String>(String.class, "text").setLocal(true)).getAll();

            assert false;
        }
        catch (CacheException e) {
            X.println("Caught expected exception: " + e);
        }
        catch (Exception ignored) {
            assert false;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanQuery() throws Exception {
        IgniteCache<Integer, String> cache = grid().getOrCreateCache(String.class.getSimpleName());

        try {
            cache.query(new ScanQuery<>(new IgniteBiPredicate<Integer, String>() {
                @Override public boolean apply(Integer id, String s) {
                    return s.equals("");
                }
            })).getAll();
        }
        catch (IgniteException ignored) {
            assertTrue("Scan query should work with disabled query indexing.", false);
        }
    }

    /**
     * Value object class.
     */
    private static class SqlValue {
        /**
         * Value.
         */
        @QuerySqlField
        private final int val;

        /**
         * @param val Value.
         */
        SqlValue(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SqlValue.class, this);
        }
    }
}
