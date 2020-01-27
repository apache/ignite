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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.verify.ValidateIndexesClosure;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXTRA_INDEX_REBUILD_LOGGING;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;

/**
 *
 */
public class RebuildIndexTest extends GridCommonAbstractTest {
    /** Rebalance cache name. */
    private static final String CACHE_NAME = "cache_name";

    /**
     * User key.
     */
    private static class UserKey {
        /** A. */
        private int account;

        /**
         * @param a A.
         */
        public UserKey(int a) {
            this.account = a;
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

        cfg.setFailureDetectionTimeout(1000000000L);

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
                .setCheckpointFrequency(10000000)
                .setWalSegmentSize(4 * 1024 * 1024)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setInitialSize(50L * 1024 * 1024)
                        .setMaxSize(50L * 1024 * 1024)
                )
        );
        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        System.setProperty(IGNITE_ENABLE_EXTRA_INDEX_REBUILD_LOGGING, "true");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(IGNITE_ENABLE_EXTRA_INDEX_REBUILD_LOGGING);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebuildIndex() throws Exception {
        IgniteEx node1 = startGrid(0);
        startGrid(1);

        node1.cluster().active(true);

        IgniteCache<UserKey, UserValue> cache = node1.getOrCreateCache(CACHE_NAME);

        cache.put(new UserKey(1), new UserValue(333));
        cache.put(new UserKey(2), new UserValue(555));

        stopGrid(0);

        removeIndexBin(0);

        IgniteEx node2 = startGrid(0);

        awaitPartitionMapExchange();

        final IgniteCacheDatabaseSharedManager db = node2.context().cache().context().database();
        while (IdleVerifyUtility.isCheckpointNow(db))
            doSleep(500);

        // Validate indexes on start.
        ValidateIndexesClosure clo = new ValidateIndexesClosure(Collections.singleton(CACHE_NAME), 0, 0);
        node2.context().resource().injectGeneric(clo);
        VisorValidateIndexesJobResult res = clo.call();

        assertFalse(res.hasIssues());
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
}
