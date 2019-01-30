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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public class IgniteCacheReadFromBackupTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetFromBackupStoreReadThroughEnabled() throws Exception {
        checkGetFromBackupStoreReadThroughEnabled(cacheConfigurations());
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10274")
    @Test
    public void testMvccGetFromBackupStoreReadThroughEnabled() throws Exception {
        checkGetFromBackupStoreReadThroughEnabled(mvccCacheConfigurations());
    }

    /**
     * @throws Exception If failed.
     */
    private void checkGetFromBackupStoreReadThroughEnabled(List<CacheConfiguration<Object, Object>> cacheCfgs) throws Exception {
        for (CacheConfiguration<Object, Object> ccfg : cacheCfgs) {
            ccfg.setCacheStoreFactory(new TestStoreFactory());
            ccfg.setReadThrough(true);

            boolean near = (ccfg.getNearConfiguration() != null);

            log.info("Test cache [mode=" + ccfg.getCacheMode() +
                ", atomicity=" + ccfg.getAtomicityMode() +
                ", backups=" + ccfg.getBackups() +
                ", near=" + near + "]");

            ignite(0).createCache(ccfg);

            awaitPartitionMapExchange();

            try {
                for (int i = 0; i < NODES; i++) {
                    Ignite ignite = ignite(i);

                    log.info("Check node: " + ignite.name());

                    IgniteCache<Integer, Integer> cache = ignite.cache(ccfg.getName());

                    TestRecordingCommunicationSpi spi = recordGetRequests(ignite, near);

                    Integer key = backupKey(cache);

                    assertNull(cache.get(key));

                    List<Object> msgs = spi.recordedMessages(false);

                    assertEquals(1, msgs.size());
                }
            }
            finally {
                ignite(0).destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetFromBackupStoreReadThroughDisabled() throws Exception {
        checkGetFromBackupStoreReadThroughDisabled(cacheConfigurations());
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10274")
    @Test
    public void testMvccGetFromBackupStoreReadThroughDisabled() throws Exception {
        checkGetFromBackupStoreReadThroughDisabled(mvccCacheConfigurations());
    }

    /**
     * @throws Exception If failed.
     */
    private void checkGetFromBackupStoreReadThroughDisabled(List<CacheConfiguration<Object, Object>> cacheCfgs) throws Exception {
        for (CacheConfiguration<Object, Object> ccfg : cacheCfgs) {
            ccfg.setCacheStoreFactory(new TestStoreFactory());
            ccfg.setReadThrough(false);

            boolean near = (ccfg.getNearConfiguration() != null);

            log.info("Test cache [mode=" + ccfg.getCacheMode() +
                ", atomicity=" + ccfg.getAtomicityMode() +
                ", backups=" + ccfg.getBackups() +
                ", near=" + near + "]");

            ignite(0).createCache(ccfg);

            awaitPartitionMapExchange();

            try {
                checkLocalRead(NODES, ccfg);
            }
            finally {
                ignite(0).destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetFromPrimaryPreloadInProgress() throws Exception {
        checkGetFromPrimaryPreloadInProgress(cacheConfigurations());
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10274")
    @Test
    public void testMvccGetFromPrimaryPreloadInProgress() throws Exception {
        checkGetFromPrimaryPreloadInProgress(mvccCacheConfigurations());
    }

    /**
     * @throws Exception If failed.
     */
    private void checkGetFromPrimaryPreloadInProgress(List<CacheConfiguration<Object, Object>> cacheCfgs) throws Exception {
        for (final CacheConfiguration<Object, Object> ccfg : cacheCfgs) {
            boolean near = (ccfg.getNearConfiguration() != null);

            log.info("Test cache [mode=" + ccfg.getCacheMode() +
                ", atomicity=" + ccfg.getAtomicityMode() +
                ", backups=" + ccfg.getBackups() +
                ", near=" + near + "]");

            ignite(0).createCache(ccfg);

            awaitPartitionMapExchange();

            try {
                Map<Ignite, Integer> backupKeys = new HashMap<>();
                Map<Ignite, Integer> nearKeys = new HashMap<>();

                for (int i = 0; i < NODES; i++) {
                    Ignite ignite = ignite(i);

                    IgniteCache<Integer, Integer> cache = ignite.cache(ccfg.getName());

                    backupKeys.put(ignite, backupKey(cache));

                    if (ccfg.getCacheMode() == PARTITIONED)
                        nearKeys.put(ignite, nearKey(cache));

                    TestRecordingCommunicationSpi spi =
                        (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

                    final int grpId = groupIdForCache(ignite, ccfg.getName());

                    spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                        @Override public boolean apply(ClusterNode node, Message msg) {
                            if (!msg.getClass().equals(GridDhtPartitionSupplyMessage.class))
                                return false;

                            return ((GridDhtPartitionSupplyMessage)msg).groupId() == grpId;
                        }
                    });
                }

                try (Ignite newNode = startGrid(NODES)) {
                    IgniteCache<Integer, Integer> cache = newNode.cache(ccfg.getName());

                    TestRecordingCommunicationSpi newNodeSpi = recordGetRequests(newNode, near);

                    Integer key = backupKey(cache);

                    assertNull(cache.get(key));

                    List<Object> msgs = newNodeSpi.recordedMessages(false);

                    assertEquals(1, msgs.size());

                    for (int i = 0; i < NODES; i++) {
                        Ignite ignite = ignite(i);

                        log.info("Check node: " + ignite.name());

                        checkLocalRead(ignite, ccfg, backupKeys.get(ignite), nearKeys.get(ignite));
                    }

                    for (int i = 0; i < NODES; i++) {
                        Ignite ignite = ignite(i);

                        TestRecordingCommunicationSpi spi =
                            (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

                        spi.stopBlock(true);
                    }

                    awaitPartitionMapExchange();

                    checkLocalRead(NODES + 1, ccfg);
                }
            }
            finally {
                ignite(0).destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoPrimaryReadPreloadFinished() throws Exception {
        checkNoPrimaryReadPreloadFinished(cacheConfigurations());
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10274")
    @Test
    public void testMvccNoPrimaryReadPreloadFinished() throws Exception {
        checkNoPrimaryReadPreloadFinished(mvccCacheConfigurations());

    }

    /**
     * @throws Exception If failed.
     */
    private void checkNoPrimaryReadPreloadFinished(List<CacheConfiguration<Object, Object>> cacheCfgs) throws Exception {
        for (CacheConfiguration<Object, Object> ccfg : cacheCfgs) {
            boolean near = (ccfg.getNearConfiguration() != null);

            log.info("Test cache [mode=" + ccfg.getCacheMode() +
                ", atomicity=" + ccfg.getAtomicityMode() +
                ", backups=" + ccfg.getBackups() +
                ", near=" + near + "]");

            ignite(0).createCache(ccfg);

            awaitPartitionMapExchange();

            try {
                checkLocalRead(NODES, ccfg);
            }
            finally {
                ignite(0).destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @param nodes Number of nodes.
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void checkLocalRead(int nodes, CacheConfiguration<Object, Object> ccfg) throws Exception {
        for (int i = 0; i < nodes; i++) {
            Ignite ignite = ignite(i);

            log.info("Check node: " + ignite.name());

            IgniteCache<Integer, Integer> cache = ignite.cache(ccfg.getName());

            List<Integer> backupKeys = backupKeys(cache, 2, 0);

            Integer backupKey = backupKeys.get(0);

            Integer nearKey = ccfg.getCacheMode() == PARTITIONED ? nearKey(cache) : null;

            checkLocalRead(ignite, ccfg, backupKey, nearKey);

            Set<Integer> keys = new HashSet<>(backupKeys);

            Map<Integer, Integer> vals = cache.getAll(keys);

            for (Integer key : keys)
                assertNull(vals.get(key));

            TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

            List<Object> msgs = spi.recordedMessages(false);

            assertEquals(0, msgs.size());
        }
    }

    /**
     * @param ignite Node.
     * @param ccfg Cache configuration.
     * @param backupKey Backup key.
     * @param nearKey Near key.
     * @throws Exception If failed.
     */
    private void checkLocalRead(Ignite ignite,
        CacheConfiguration<Object, Object> ccfg,
        Integer backupKey,
        Integer nearKey) throws Exception {
        IgniteCache<Integer, Integer> cache = ignite.cache(ccfg.getName());

        TestRecordingCommunicationSpi spi = recordGetRequests(ignite, ccfg.getNearConfiguration() != null);

        List<Object> msgs;

        if (nearKey != null) {
            assertNull(cache.get(nearKey));

            msgs = spi.recordedMessages(false);

            assertEquals(1, msgs.size());
        }

        assertNull(cache.get(backupKey));

        msgs = spi.recordedMessages(false);

        assertTrue(msgs.isEmpty());
    }

    /**
     * @param ignite Node.
     * @param near Near cache flag.
     * @return Communication SPI.
     */
    private TestRecordingCommunicationSpi recordGetRequests(Ignite ignite, boolean near) {
        TestRecordingCommunicationSpi spi =
            (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

        spi.record(near ? GridNearGetRequest.class : GridNearSingleGetRequest.class);

        return spi;
    }

    /**
     * @return Cache configurations to test.
     */
    private List<CacheConfiguration<Object, Object>> cacheConfigurations() {
        List<CacheConfiguration<Object, Object>> ccfgs = new ArrayList<>();

        ccfgs.add(cacheConfiguration(REPLICATED, ATOMIC, 0, false));
        ccfgs.add(cacheConfiguration(REPLICATED, TRANSACTIONAL, 0, false));

        ccfgs.add(cacheConfiguration(PARTITIONED, ATOMIC, 1, false));
        ccfgs.add(cacheConfiguration(PARTITIONED, ATOMIC, 1, true));
        ccfgs.add(cacheConfiguration(PARTITIONED, ATOMIC, 2, false));

        ccfgs.add(cacheConfiguration(PARTITIONED, TRANSACTIONAL, 1, false));
        ccfgs.add(cacheConfiguration(PARTITIONED, TRANSACTIONAL, 1, true));
        ccfgs.add(cacheConfiguration(PARTITIONED, TRANSACTIONAL, 2, false));

        return ccfgs;
    }

    /**
     * @return Cache configurations to test.
     */
    private List<CacheConfiguration<Object, Object>> mvccCacheConfigurations() {
        List<CacheConfiguration<Object, Object>> ccfgs = new ArrayList<>();

        ccfgs.add(cacheConfiguration(REPLICATED, TRANSACTIONAL_SNAPSHOT, 0, false));

        ccfgs.add(cacheConfiguration(PARTITIONED, TRANSACTIONAL_SNAPSHOT, 1, false));
        ccfgs.add(cacheConfiguration(PARTITIONED, TRANSACTIONAL_SNAPSHOT, 1, true));
        ccfgs.add(cacheConfiguration(PARTITIONED, TRANSACTIONAL_SNAPSHOT, 2, false));

        return ccfgs;
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Cache atomicity mode.
     * @param backups Number of backups.
     * @param nearEnabled {@code True} if near cache should be enabled.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        int backups,
        boolean nearEnabled) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(atomicityMode);

        if (cacheMode != REPLICATED) {
            ccfg.setBackups(backups);

            if (nearEnabled)
                ccfg.setNearConfiguration(new NearCacheConfiguration<>());
        }

        return ccfg;
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStore<Object, Object>> {
        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public CacheStore<Object, Object> create() {
            return new CacheStoreAdapter() {
                @Override public Object load(Object key) throws CacheLoaderException {
                    return null;
                }

                @Override public void write(Cache.Entry entry) throws CacheWriterException {
                    // No-op.
                }

                @Override public void delete(Object key) throws CacheWriterException {
                    // No-op.
                }
            };
        }
    }
}
