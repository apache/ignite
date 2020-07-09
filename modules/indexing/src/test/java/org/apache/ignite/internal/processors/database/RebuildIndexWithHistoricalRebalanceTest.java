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

package org.apache.ignite.internal.processors.database;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.verify.ValidateIndexesClosure;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;

/**
 * Rebuild index after index.bin remove, when partition is moving.
 */
public class RebuildIndexWithHistoricalRebalanceTest extends GridCommonAbstractTest {
    /** Rebalance cache name. */
    private static final String CACHE_NAME = "cache_name";

    /** Supply message latch. */
    private static final AtomicReference<CountDownLatch> SUPPLY_MESSAGE_LATCH = new AtomicReference<>();

    /** Test logger. */
    private final ListeningTestLogger log = new ListeningTestLogger(false, super.log);

    /**
     * User key.
     */
    private static class UserKey {
        /** A. */
        private int account;

        /**
         * @param a A.
         */
        public UserKey(int account) {
            this.account = account;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "UserKey{" +
                "account=" + account +
                '}';
        }
    }

    /**
     * User value.
     */
    private static class UserValue {
        /** balance. */
        private int balance;

        /**
         * @param balance balance.
         */
        public UserValue(int balance) {
            this.balance = balance;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "UserValue{" +
                "balance=" + balance +
                '}';
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);
        cfg.setGridLogger(log);

        QueryEntity qryEntity = new QueryEntity();
        qryEntity.setKeyType(UserKey.class.getName());
        qryEntity.setValueType(UserValue.class.getName());
        qryEntity.setKeyFields(new HashSet<>(Arrays.asList("account")));

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("account", "java.lang.Integer");
        fields.put("balance", "java.lang.Integer");
        qryEntity.setFields(fields);

        QueryIndex idx1 = new QueryIndex();
        idx1.setName("IDX_1");
        idx1.setIndexType(QueryIndexType.SORTED);
        LinkedHashMap<String, Boolean> idxFields = new LinkedHashMap<>();
        idxFields.put("account", false);
        idxFields.put("balance", false);
        idx1.setFields(idxFields);

        QueryIndex idx2 = new QueryIndex();
        idx2.setName("IDX_2");
        idx2.setIndexType(QueryIndexType.SORTED);
        idxFields = new LinkedHashMap<>();
        idxFields.put("balance", false);
        idx2.setFields(idxFields);

        qryEntity.setIndexes(Arrays.asList(idx1, idx2));

        cfg.setCacheConfiguration(new CacheConfiguration<UserKey, UserValue>()
            .setName(CACHE_NAME)
            .setBackups(2)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(REPLICATED)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setOnheapCacheEnabled(true)
            .setEvictionPolicy(new FifoEvictionPolicy(1000))
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setQueryEntities(Collections.singleton(qryEntity)));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setInitialSize(200L * 1024 * 1024)
                        .setMaxSize(200L * 1024 * 1024)
                )
        );

        cfg.setCommunicationSpi(new RebalanceBlockingSPI());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        log.clearListeners();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0") // Use only historical rebalance
    public void shouldRebuldIndexForMovingPartitionWithHistoricalRebalance() throws Exception {
        IgniteEx node1 = startGrid(0);
        startGrid(1);

        node1.cluster().active(true);

        IgniteCache<UserKey, UserValue> cache = node1.getOrCreateCache(CACHE_NAME);

        cache.put(new UserKey(1), new UserValue(333));

        stopGrid(1);

        cache.put(new UserKey(2), new UserValue(555));

        SUPPLY_MESSAGE_LATCH.set(new CountDownLatch(1));

        removeIndexBin(1);

        LogListener rebuildLsnr = finishIndexRebuildLsnr(CACHE_NAME);

        IgniteEx node2 = startGrid(1);

        assertTrue(GridTestUtils.waitForCondition(rebuildLsnr::check, 10_000));

        SUPPLY_MESSAGE_LATCH.get().countDown();

        awaitPartitionMapExchange();

        ValidateIndexesClosure clo = new ValidateIndexesClosure(Collections.singleton(CACHE_NAME), 0, 0, false, true);
        node2.context().resource().injectGeneric(clo);
        VisorValidateIndexesJobResult res = clo.call();

        assertFalse(res.hasIssues());
    }

    /** */
    private LogListener finishIndexRebuildLsnr(String cacheName) {
        LogListener lsnr = LogListener.matches(s -> s.startsWith("Finished indexes rebuilding for cache [name=" + cacheName)).times(1).build();

        log.registerListener(lsnr);

        return lsnr;
    }

    /** */
    private void removeIndexBin(int nodeId) throws IgniteCheckedException {
        U.delete(
            U.resolveWorkDirectory(
                U.defaultWorkDirectory(),
                "db/" + U.maskForFileName(getTestIgniteInstanceName(nodeId)) + "/cache-" + CACHE_NAME + "/" + INDEX_FILE_NAME,
                false
            )
        );
    }

    /** */
    private static class RebalanceBlockingSPI extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                int grpId = ((GridCacheGroupIdMessage)((GridIoMessage)msg).message()).groupId();

                if (grpId == CU.cacheId(CACHE_NAME)) {
                    CountDownLatch latch0 = SUPPLY_MESSAGE_LATCH.get();

                    if (latch0 != null)
                        try {
                            latch0.await();
                        }
                        catch (InterruptedException ex) {
                            throw new IgniteException(ex);
                        }
                }
            }

            super.sendMessage(node, msg);
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                int grpId = ((GridCacheGroupIdMessage)((GridIoMessage)msg).message()).groupId();

                if (grpId == CU.cacheId(CACHE_NAME)) {
                    CountDownLatch latch0 = SUPPLY_MESSAGE_LATCH.get();

                    if (latch0 != null)
                        try {
                            latch0.await();
                        }
                        catch (InterruptedException ex) {
                            throw new IgniteException(ex);
                        }
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
