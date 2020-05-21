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
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.verify.ValidateIndexesClosure;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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

    /** Server listening logger. */
    private ListeningTestLogger srvLog;

    /** */
    private static final Pattern idxRebuildPattert = Pattern.compile(
        "Details for cache rebuilding \\[name=cache_name, grpName=null].*" +
            "Scanned rows 2, visited types \\[UserValue].*" +
            "Type name=UserValue.*" +
            "Index: name=_key_PK, size=2.*" +
            "Index: name=IDX_2, size=2.*" +
            "Index: name=IDX_1, size=2.*",
        Pattern.DOTALL);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);
        cfg.setGridLogger(log);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("account", "java.lang.Integer");
        fields.put("balance", "java.lang.Integer");

        QueryEntity qryEntity = new QueryEntity()
            .setKeyType(UserKey.class.getName())
            .setValueType(UserValue.class.getName())
            .setKeyFields(new HashSet<>(Arrays.asList("account")))
            .setFields(fields);

        LinkedHashMap<String, Boolean> idxFields = new LinkedHashMap<>();
        idxFields.put("account", false);
        idxFields.put("balance", false);

        QueryIndex idx1 = new QueryIndex(idxFields, QueryIndexType.SORTED).setName("IDX_1");
        QueryIndex idx2 = new QueryIndex("balance", QueryIndexType.SORTED, false, "IDX_2");

        qryEntity.setIndexes(Arrays.asList(idx1, idx2));

        cfg.setCacheConfiguration(new CacheConfiguration<UserKey, UserValue>()
            .setName(CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(REPLICATED)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setQueryEntities(Collections.singleton(qryEntity)));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setCheckpointFrequency(10000000)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
        );

        if (srvLog != null)
            cfg.setGridLogger(srvLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        srvLog = null;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_ENABLE_EXTRA_INDEX_REBUILD_LOGGING, value = "true")
    public void testRebuildIndexWithLogging() throws Exception {
        check(true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_ENABLE_EXTRA_INDEX_REBUILD_LOGGING, value = "false")
    public void testRebuildIndexWithoutLogging() throws Exception {
        check(false);
    }

    /**
     * @throws Exception if failed.
     */
    private void check(boolean msgFound) throws Exception {
        srvLog = new ListeningTestLogger(false, log);

        LogListener idxRebuildLsnr = LogListener.matches(idxRebuildPattert).build();
        srvLog.registerListener(idxRebuildLsnr);

        IgniteEx node = startGrids(2);

        node.cluster().active(true);

        IgniteCache<UserKey, UserValue> cache = node.getOrCreateCache(CACHE_NAME);

        cache.put(new UserKey(1), new UserValue(333));
        cache.put(new UserKey(2), new UserValue(555));

        stopGrid(0);

        removeIndexBin(0);

        node = startGrid(0);

        awaitPartitionMapExchange();

        forceCheckpoint();

        enableCheckpoints(G.allGrids(), false);

        // Validate indexes on start.
        ValidateIndexesClosure clo = new ValidateIndexesClosure(Collections.singleton(CACHE_NAME), 0, 0, false, true);

        node.context().resource().injectGeneric(clo);

        assertFalse(clo.call().hasIssues());

        assertEquals(msgFound, idxRebuildLsnr.check());
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

    /**
     * User key.
     */
    private static class UserKey {
        /** Account. */
        private int account;

        /**
         * @param a A.
         */
        public UserKey(int a) {
            this.account = a;
        }
    }

    /**
     * User value.
     */
    private static class UserValue {
        /** Balance. */
        private int balance;

        /**
         * @param balance balance.
         */
        public UserValue(int balance) {
            this.balance = balance;
        }
    }
}
