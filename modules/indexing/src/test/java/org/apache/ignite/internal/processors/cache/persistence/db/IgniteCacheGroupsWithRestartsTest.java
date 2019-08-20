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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Testing corner cases in cache group functionality: -stopping cache in shared group and immediate node leaving;
 * -starting cache in shared group with the same name as destroyed one; -etc.
 */
@SuppressWarnings({"unchecked", "ThrowableNotThrown"})
public class IgniteCacheGroupsWithRestartsTest extends GridCommonAbstractTest {
    /**
     *
     */
    private volatile boolean startExtraStaticCache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(gridName);

        configuration.setConsistentId(gridName);

        configuration.setConnectorConfiguration(new ConnectorConfiguration());

        DataStorageConfiguration cfg = new DataStorageConfiguration();

        cfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(256 * 1024 * 1024));

        configuration.setDataStorageConfiguration(cfg);

        if (startExtraStaticCache)
            configuration.setCacheConfiguration(getCacheConfiguration(3));

        return configuration;
    }

    /**
     * @param i Cache index number.
     * @return Cache configuration with the given number in name.
     */
    private CacheConfiguration<Object, Object> getCacheConfiguration(int i) {
        CacheConfiguration ccfg = new CacheConfiguration();

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("updateDate", "java.lang.Date");
        fields.put("amount", "java.lang.Long");
        fields.put("name", "java.lang.String");

        Set<QueryIndex> indices = Collections.singleton(new QueryIndex("name", QueryIndexType.SORTED));

        ccfg.setName(getCacheName(i))
            .setGroupName("group")
            .setQueryEntities(Collections.singletonList(
                new QueryEntity(Long.class, Account.class)
                    .setFields(fields)
                    .setIndexes(indices)
            ))
            .setAffinity(new RendezvousAffinityFunction(false, 64));

        return ccfg;
    }

    /**
     * @param i Index.
     * @return Generated cache name for index.
     */
    private String getCacheName(int i) {
        return "cache-" + i;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8717")
    @Test
    public void testNodeRestartRightAfterCacheStop() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8717");

        IgniteEx ex = startGrids(3);

        prepareCachesAndData(ex);

        ex.destroyCache(getCacheName(0));

        assertNull(ex.cachex(getCacheName(0)));

        stopGrid(2, true);

        startGrid(2);

        assertNull(ex.cachex(getCacheName(0)));

        IgniteCache<Object, Object> cache = ex.createCache(getCacheConfiguration(0));

        awaitPartitionMapExchange();

        assertEquals(0, cache.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeRestartBetweenCacheStop() throws Exception {
        IgniteEx ex = startGrids(3);

        prepareCachesAndData(ex);

        stopGrid(2, true);

        ex.destroyCache(getCacheName(0));

        assertNull(ex.cachex(getCacheName(0)));

        try {
            startGrid(2);

            fail();
        }
        catch (Exception e) {
            List<Throwable> list = X.getThrowableList(e);

            assertTrue(list.stream().
                anyMatch(x -> x.getMessage().
                    contains("Joining node has caches with data which are not presented on cluster")));
        }

        removeCacheDir(getTestIgniteInstanceName(2), "cacheGroup-group");

        IgniteEx node2 = startGrid(2);

        assertEquals(3, node2.cluster().nodes().size());
    }

    /**
     * @param instanceName Instance name.
     * @param cacheGroup Cache group.
     */
    private void removeCacheDir(String instanceName, String cacheGroup) throws IgniteCheckedException {
        String dn2DirName = instanceName.replace(".", "_");

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(),
            DFLT_STORE_DIR + "/" + dn2DirName + "/" + cacheGroup, true));
    }

    /**
     * @param ignite Ignite instance.
     */
    private void prepareCachesAndData(IgniteEx ignite) {
        ignite.cluster().active(true);

        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 64 * 10; i++) {
                IgniteCache<Object, Object> cache = ignite.getOrCreateCache(getCacheConfiguration(j));

                byte[] val = new byte[ThreadLocalRandom.current().nextInt(8148)];

                Arrays.fill(val, (byte)i);

                cache.put((long)i, new Account(i));
            }
        }
    }

    /**
     *
     */
    static class Account {
        /**
         *
         */
        private final int val;

        /**
         * @param val Value.
         */
        public Account(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Account.class, this);
        }
    }
}
