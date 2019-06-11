/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_READ_LOAD_BALANCING;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS_OVERRIDE;

/**
 *
 */
public class NearCacheNoMessagesTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        if (igniteInstanceName.contains("client"))
            cfg.setClientMode(true);

        Map<String, Object> attrs = new HashMap<>();

        attrs.put(ATTR_MACS_OVERRIDE, igniteInstanceName);

        cfg.setUserAttributes(attrs);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(4);

        startGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    @Test
    public void testNearGetsDoNotTriggerNetworkCommunication_Transactional_Replicated() {
        testNoMessage(CacheAtomicityMode.TRANSACTIONAL, CacheMode.REPLICATED);
    }

    /**
     *
     */
    @Test
    public void testNearGetsDoNotTriggerNetworkCommunication_Transactional_Partitioned() {
        testNoMessage(CacheAtomicityMode.TRANSACTIONAL, CacheMode.PARTITIONED);
    }

    /**
     *
     */
    @Test
    public void testNearGetsDoNotTriggerNetworkCommunication_Atomic_Replicated() {
        testNoMessage(CacheAtomicityMode.ATOMIC, CacheMode.REPLICATED);
    }

    /**
     *
     */
    @Test
    public void testNearGetsDoNotTriggerNetworkCommunication_Atomic_Partitioned() {
        testNoMessage(CacheAtomicityMode.ATOMIC, CacheMode.PARTITIONED);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param cacheMode Cache mode.
     */
    private void testNoMessage(CacheAtomicityMode atomicityMode, CacheMode cacheMode) {
        testNoMessage(atomicityMode, cacheMode, true, true);
        testNoMessage(atomicityMode, cacheMode, true, false);
        testNoMessage(atomicityMode, cacheMode, false, true);
        testNoMessage(atomicityMode, cacheMode, false, false);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param cacheMode Cache mode.
     * @param readFromBackup Read from backup allowed or not.
     * @param readLoadBalancing Read load balancing is enabled.
     */
    private void testNoMessage(
        CacheAtomicityMode atomicityMode,
        CacheMode cacheMode,
        boolean readFromBackup,
        boolean readLoadBalancing
    ) {
        IgniteLogger log = ignite(0).log();

        if (log.isInfoEnabled())
            log.info("Starting test with parameters [atomicityMode=" + atomicityMode +
                ", cacheMode=" + cacheMode + ", readFromBackup=" + readFromBackup +
                ", readLoadBalancing=" + readLoadBalancing + "]");

        String oldValue = System.getProperty(IGNITE_READ_LOAD_BALANCING);

        System.setProperty(IGNITE_READ_LOAD_BALANCING, readLoadBalancing? "true" : "false");

        ignite(0).createCache(
            new CacheConfiguration<>("testCache")
                .setAtomicityMode(atomicityMode)
                .setCacheMode(cacheMode)
                .setBackups(1)
                .setReadFromBackup(readFromBackup));

        try {
            {
                IgniteCache<Integer, Integer> cache = ignite(0).cache("testCache");

                for (int i = 0; i < 100; i++)
                    cache.put(i, i);
            }

            IgniteCache<Object, Object> nearCache = grid("client")
                .getOrCreateNearCache("testCache", new NearCacheConfiguration<>());

            {
                for (int i = 0; i < 100; i++)
                    nearCache.get(i);
            }

            TestRecordingCommunicationSpi recordingSpi = (TestRecordingCommunicationSpi)grid("client")
                .configuration().getCommunicationSpi();

            recordingSpi.record(GridNearGetRequest.class);

            int nearSize = nearCache.size(CachePeekMode.NEAR);

            assertTrue(nearSize > 0);

            for (int i = 0; i < 100; i++)
                assertEquals(i, nearCache.get(i));

            List<Object> recorded = recordingSpi.recordedMessages(false);

            assertEquals(100 - nearSize, recorded.size());
        }
        finally {
            ignite(0).destroyCache("testCache");

            if (oldValue == null)
                System.clearProperty(IGNITE_READ_LOAD_BALANCING);
            else
                System.setProperty(IGNITE_READ_LOAD_BALANCING, oldValue);

            if (log.isInfoEnabled())
                log.info("Stopping test");
        }
    }
}