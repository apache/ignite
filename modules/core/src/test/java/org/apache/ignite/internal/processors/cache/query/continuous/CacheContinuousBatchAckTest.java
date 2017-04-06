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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_GRID_NAME;

/**
 * Continuous queries tests.
 */
public class CacheContinuousBatchAckTest extends GridCommonAbstractTest implements Serializable {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    protected static final String CLIENT = "_client";

    /** */
    protected static final String SERVER = "server";

    /** */
    protected static final String SERVER2 = "server2";

    /** */
    protected static final AtomicBoolean fail = new AtomicBoolean(false);

    /** */
    protected static final AtomicBoolean filterOn = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.endsWith(CLIENT)) {
            cfg.setClientMode(true);

            cfg.setCommunicationSpi(new FailedTcpCommunicationSpi(true, false));
        }
        else if (gridName.endsWith(SERVER2))
            cfg.setCommunicationSpi(new FailedTcpCommunicationSpi(false, true));
        else
            cfg.setCommunicationSpi(new FailedTcpCommunicationSpi(false, false));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(SERVER);
        startGrid(SERVER2);
        startGrid("1" + CLIENT);
        startGrid("2" + CLIENT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        fail.set(false);

        filterOn.set(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartition() throws Exception {
        checkBackupAcknowledgeMessage(cacheConfiguration(PARTITIONED, 1, ATOMIC, ONHEAP_TIERED, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionWithFilter() throws Exception {
        filterOn.set(true);

        checkBackupAcknowledgeMessage(cacheConfiguration(PARTITIONED, 1, ATOMIC, ONHEAP_TIERED, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionNoBackups() throws Exception {
        checkBackupAcknowledgeMessage(cacheConfiguration(PARTITIONED, 0, ATOMIC, ONHEAP_TIERED, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionTx() throws Exception {
        checkBackupAcknowledgeMessage(cacheConfiguration(PARTITIONED, 1, TRANSACTIONAL, ONHEAP_TIERED, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionTxWithFilter() throws Exception {
        filterOn.set(true);

        checkBackupAcknowledgeMessage(cacheConfiguration(PARTITIONED, 1, TRANSACTIONAL, ONHEAP_TIERED, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionTxNoBackup() throws Exception {
        checkBackupAcknowledgeMessage(cacheConfiguration(PARTITIONED, 0, TRANSACTIONAL, ONHEAP_TIERED, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionTxNoBackupWithFilter() throws Exception {
        filterOn.set(true);

        checkBackupAcknowledgeMessage(cacheConfiguration(PARTITIONED, 0, TRANSACTIONAL, ONHEAP_TIERED, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionOffheap() throws Exception {
        checkBackupAcknowledgeMessage(cacheConfiguration(PARTITIONED, 1, ATOMIC, OFFHEAP_TIERED, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionOffheapWithFilter() throws Exception {
        filterOn.set(true);

        checkBackupAcknowledgeMessage(cacheConfiguration(PARTITIONED, 1, ATOMIC, OFFHEAP_TIERED, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionTxOffheap() throws Exception {
        checkBackupAcknowledgeMessage(cacheConfiguration(PARTITIONED, 1, TRANSACTIONAL, OFFHEAP_TIERED, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicated() throws Exception {
        checkBackupAcknowledgeMessage(cacheConfiguration(REPLICATED, 1, ATOMIC, ONHEAP_TIERED, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedTx() throws Exception {
        checkBackupAcknowledgeMessage(cacheConfiguration(REPLICATED, 1, TRANSACTIONAL, ONHEAP_TIERED, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedTxWithFilter() throws Exception {
        filterOn.set(true);

        checkBackupAcknowledgeMessage(cacheConfiguration(REPLICATED, 1, TRANSACTIONAL, ONHEAP_TIERED, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedOffheap() throws Exception {
        checkBackupAcknowledgeMessage(cacheConfiguration(REPLICATED, 1, ATOMIC, OFFHEAP_TIERED, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedTxOffheap() throws Exception {
        checkBackupAcknowledgeMessage(cacheConfiguration(REPLICATED, 1, TRANSACTIONAL, OFFHEAP_TIERED, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedTxOffheapWithFilter() throws Exception {
        filterOn.set(true);

        checkBackupAcknowledgeMessage(cacheConfiguration(REPLICATED, 1, TRANSACTIONAL, OFFHEAP_TIERED, true));
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void checkBackupAcknowledgeMessage(CacheConfiguration<Object, Object> ccfg) throws Exception {
        QueryCursor qry = null;

        IgniteCache<Object, Object> cache = null;

        try {
            ContinuousQuery q = new ContinuousQuery();

            q.setLocalListener(new CacheEntryUpdatedListener() {
                @Override public void onUpdated(Iterable iterable) throws CacheEntryListenerException {
                    // No-op.
                }
            });

            cache = grid(SERVER).getOrCreateCache(ccfg);

            qry = cache.query(q);

            for (int i = 0; i < 10000; i++)
                cache.put(i, i);

            assert !GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    return fail.get();
                }
            }, 1300L);
        }
        finally {
            if (qry != null)
                qry.close();

            if (cache != null)
                grid(SERVER).destroyCache(cache.getName());
        }
    }

    /**
     *
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param atomicityMode Cache atomicity mode.
     * @param memoryMode Cache memory mode.
     * @param filter Filter enabled.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        int backups,
        CacheAtomicityMode atomicityMode,
        CacheMemoryMode memoryMode, boolean filter) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setMemoryMode(memoryMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        if (filter)
            ccfg.setNodeFilter(new P1<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    return !node.attributes().get(ATTR_GRID_NAME).equals(SERVER2);
                }
            });

        return ccfg;
    }

    /**
     *
     */
    protected static class FailedTcpCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private boolean check;

        /** */
        private boolean periodicCheck;

        /**
         * @param alwaysCheck Always check inbound message.
         * @param periodicCheck Check when {@code filterOn} enabled.
         */
        public FailedTcpCommunicationSpi(boolean alwaysCheck, boolean periodicCheck) {
            this.check = alwaysCheck;
            this.periodicCheck = periodicCheck;
        }

        /** {@inheritDoc} */
        @Override protected void notifyListener(UUID sndId, Message msg, IgniteRunnable msgC) {
            if (check || (periodicCheck && filterOn.get())) {
                if (msg instanceof GridIoMessage &&
                    ((GridIoMessage)msg).message() instanceof CacheContinuousQueryBatchAck)
                    fail.set(true);
            }

            super.notifyListener(sndId, msg, msgC);
        }
    }
}
