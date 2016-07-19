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

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests SQL query runnable API.
 */
public class IgniteCacheSqlQueryRunnableSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration<?, ?> cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setSwapEnabled(false);
        cacheCfg.setBackups(0);
        cacheCfg.setIndexedTypes(
            Integer.class, Integer.class
        );

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        ignite(0).cache(null).removeAll();
    }

    /** */
    public void testQueryExecution() throws Exception {
        IgniteEx ignite = grid(0);

        IgniteCache<Integer, Integer> c = ignite.cache(null);

        int total = 10_000;

        for (int i = 0; i < total; i++)
            c.put(i, i);

        IgniteH2Indexing indexing = U.field(U.field(ignite.context(), "qryProc"), "idx");

        final RunnableFuture<ResultSet> fut = indexing.sqlQueryFuture(
            null,
            indexing.connectionForSpace(null),
            "select _key, _val from Integer",
            Collections.emptyList(),
            false,
            0);

        multithreadedAsync(new Runnable() {
            @Override public void run() {
                fut.run();
            }
        }, 1);

        ResultSet rs = fut.get();
        int cnt = 0;
        while (rs.next())
            cnt++;

        assertEquals(total, cnt);
    }

    /** */
    public void testQueryCancellation() throws Exception {
        IgniteEx ignite = grid(0);

        IgniteCache<Integer, Integer> c = ignite.cache(null);

        int total = 10_000;

        for (int i = 0; i < total; i++)
            c.put(i, i);

        IgniteH2Indexing indexing = U.field(U.field(ignite.context(), "qryProc"), "idx");

        final RunnableFuture<ResultSet> fut = indexing.sqlQueryFuture(
            null,
            indexing.connectionForSpace(null),
            "select t1._key, t2._key from Integer t1, Integer t2",
            Collections.emptyList(),
            false,
            0);

        ignite.scheduler().runLocal(new Runnable() {
            @Override public void run() {
                fut.cancel(false);
            }
        }, 3, TimeUnit.SECONDS);

        multithreadedAsync(new Runnable() {
            @Override public void run() {
                fut.run();
            }
        }, 1);

        try {
            ResultSet set = fut.get();

            fail("Future was cancelled.");
        }
        catch (CancellationException e) {
            // No-op.
        }
        catch (Exception e) {
            fail("Expecting cancellation exception");
        }
    }

    /** */
    public void testQueryTimeout() throws Exception {
        IgniteEx ignite = grid(0);

        IgniteCache<Integer, Integer> c = ignite.cache(null);

        int total = 10_000;

        for (int i = 0; i < total; i++)
            c.put(i, i);

        IgniteH2Indexing indexing = U.field(U.field(ignite.context(), "qryProc"), "idx");

        final RunnableFuture<ResultSet> fut = indexing.sqlQueryFuture(
            null,
            indexing.connectionForSpace(null),
            "select t1._key, t2._key from Integer t1, Integer t2",
            Collections.emptyList(),
            false,
            3);

        multithreadedAsync(new Runnable() {
            @Override public void run() {
                fut.run();
            }
        }, 1);

        try {
            ResultSet set = fut.get();

            fail("Future was cancelled.");
        }
        catch (CancellationException e) {
            // No-op.
        }
        catch (Exception e) {
            fail("Expecting cancellation exception");
        }
    }

    /** */
    public void testQueryCancelWithTimeout() throws Exception {
        IgniteEx ignite = grid(0);

        IgniteCache<Integer, Integer> c = ignite.cache(null);

        int total = 10_000;

        for (int i = 0; i < total; i++)
            c.put(i, i);

        IgniteH2Indexing indexing = U.field(U.field(ignite.context(), "qryProc"), "idx");

        final RunnableFuture<ResultSet> fut = indexing.sqlQueryFuture(
            null,
            indexing.connectionForSpace(null),
            "select t1._key, t2._key from Integer t1, Integer t2",
            Collections.emptyList(),
            false,
            6);

        ignite.scheduler().runLocal(new Runnable() {
            @Override public void run() {
                fut.cancel(false);
            }
        }, 3, TimeUnit.SECONDS);

        multithreadedAsync(new Runnable() {
            @Override public void run() {
                fut.run();
            }
        }, 1);

        try {
            ResultSet set = fut.get();

            fail("Future was cancelled.");
        }
        catch (CancellationException e) {
            // No-op.
        }
        catch (Exception e) {
            fail("Expecting cancellation exception");
        }
    }
}