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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.continuous.GridContinuousHandler;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheContinuousQueryBackupQueueTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Keys count. */
    private static final int KEYS_COUNT = 1024;

    /** CQ count. */
    private static final int QUERY_COUNT = 20;

    /** Grid count. */
    private static final int GRID_COUNT = 2;

    /** */
    private static boolean client = false;

    /** */
    private static String CACHE_NAME = "test-cache";

    /** */
    private static final int BACKUP_ACK_THRESHOLD = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        cfg.setClientMode(client);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(GRID_COUNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        client = false;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TimeUnit.MINUTES.toMillis(10);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBackupQueue() throws Exception {
        final CacheEventListener lsnr = new CacheEventListener();

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setLocalListener(lsnr);
        qry.setRemoteFilterFactory(new AlwaysFalseFilterFactory());

        try (QueryCursor<?> ignore = grid(0).cache(CACHE_NAME).query(qry)) {
            for (int i = 0; i < KEYS_COUNT; i++) {
                log.info("Put key: " + i);

                for (int j = 0; j < 100; j++)
                    grid(j % GRID_COUNT).cache(CACHE_NAME).put(i, new byte[1024 * 50]);
            }

            log.info("Finish.");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testManyQueryBackupQueue() throws Exception {
        List<QueryCursor> qryCursors = new ArrayList<>();

        for (int i = 0; i < QUERY_COUNT; i++) {
            ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

            qry.setLocalListener(new CacheEventListener());
            qry.setRemoteFilterFactory(new AlwaysFalseFilterFactory());

            qryCursors.add(grid(0).cache(CACHE_NAME).query(qry));
        }

        for (int i = 0; i < KEYS_COUNT; i++) {
            log.info("Put key: " + i);

            for (int j = 0; j < 150; j++)
                grid(ThreadLocalRandom.current().nextInt(GRID_COUNT)).cache(CACHE_NAME).put(i, new byte[1024 * 50]);
        }

        int size = backupQueueSize();

        assertTrue(size > 0);
        assertTrue(size <= BACKUP_ACK_THRESHOLD * QUERY_COUNT * /* partition count */1024);

        for (QueryCursor qry : qryCursors)
            qry.close();
    }


    /**
     * @throws Exception If failed.
     */
    public void testBackupQueueAutoUnsubscribeFalse() throws Exception {
        try {
            client = true;

            Ignite client = startGrid(GRID_COUNT);

            awaitPartitionMapExchange();

            List<QueryCursor> qryCursors = new ArrayList<>();

            for (int i = 0; i < QUERY_COUNT; i++) {
                ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

                qry.setLocalListener(new CacheEventListener());
                qry.setRemoteFilterFactory(new AlwaysFalseFilterFactory());
                qry.setAutoUnsubscribe(false);

                qryCursors.add(client.cache(CACHE_NAME).query(qry));
            }

            for (int i = 0; i < KEYS_COUNT; i++) {
                log.info("Put key: " + i);

                grid(i % GRID_COUNT).cache(CACHE_NAME).put(i, new byte[1024 * 50]);
            }

            int size = backupQueueSize();

            assertTrue(size > 0);
            assertTrue(size <= BACKUP_ACK_THRESHOLD * QUERY_COUNT * /* partition count */1024);

            stopGrid(GRID_COUNT);

            awaitPartitionMapExchange();

            for (int i = 0; i < KEYS_COUNT; i++) {
                log.info("Put key: " + i);

                grid(i % GRID_COUNT).cache(CACHE_NAME).put(i, new byte[1024 * 50]);
            }

            size = backupQueueSize();

            assertEquals(-1, size);
        }
        finally {
            stopGrid(GRID_COUNT);
        }
    }

    /**
     * @return Backup queue size or {@code -1} if backup queue doesn't exist.
     */
    private int backupQueueSize() {
        int backupQueueSize = -1;

        for (int i = 0; i < GRID_COUNT; i++) {
            for (Collection<Object> backQueue : backupQueues(grid(i)))
                backupQueueSize += backQueue.size();
        }

        return backupQueueSize;
    }

    /**
     * @param ignite Ignite.
     * @return Backup queue for test query.
     */
    private List<Collection<Object>> backupQueues(Ignite ignite) {
        GridContinuousProcessor proc = ((IgniteKernal)ignite).context().continuous();

        Map<Object, Object> infos = new HashMap<>();

        Map<Object, Object> rmtInfos = GridTestUtils.getFieldValue(proc, "rmtInfos");
        Map<Object, Object> locInfos = GridTestUtils.getFieldValue(proc, "locInfos");

        infos.putAll(rmtInfos);
        infos.putAll(locInfos);

        List<Collection<Object>> backupQueues = new ArrayList<>();

        for (Object info : infos.values()) {
            GridContinuousHandler hnd = GridTestUtils.getFieldValue(info, "hnd");

            if (hnd.isQuery() && hnd.cacheName().equals(CACHE_NAME)) {
                Collection<Object> q = GridTestUtils.getFieldValue(hnd,
                    CacheContinuousQueryHandler.class, "backupQueue");

                if (q != null)
                    backupQueues.add(q);
            }
        }

        return backupQueues;
    }

    /**
     *
     */
    private static class AlwaysFalseFilterFactory implements Factory<CacheEntryEventFilter<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheEntryEventFilter<Object, Object> create() {
            return new AlwaysFalseFilter();
        }
    }

    /**
     *
     */
    private static class AlwaysFalseFilter implements CacheEntryEventFilter<Object, Object>, Serializable {
        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<?, ?> evt) {
            return false;
        }
    }

    /**
     *
     */
    private static class CacheEventListener implements CacheEntryUpdatedListener<Object, Object> {
        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
            fail();
        }
    }
}
