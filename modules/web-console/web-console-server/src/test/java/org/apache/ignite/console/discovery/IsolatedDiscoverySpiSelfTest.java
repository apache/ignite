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

package org.apache.ignite.console.discovery;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test for {@link IsolatedDiscoverySpi}.
 */
public class IsolatedDiscoverySpiSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * @param instanceName Instance name.
     * @param consistentId Consistent ID.
     * @return Ignite isolated configuration.
     */
    private IgniteConfiguration isolatedConfiguration(String instanceName, String consistentId) {
        return new IgniteConfiguration()
            .setIgniteInstanceName(instanceName)
            .setConsistentId(consistentId)
            .setDiscoverySpi(new IsolatedDiscoverySpi())
            .setCommunicationSpi(new IsolatedCommunicationSpi())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                    )
            )
            .setFailureHandler(new NoOpFailureHandler());
    }

    /**
     * Test that isolated node can execute cache operations.
     */
    @Test
    public void testCacheOperations() {
        Ignite ignite = Ignition.start(isolatedConfiguration("isolated-node", "data"));

        ignite.cluster().active(true);

        assertTrue(ignite.cluster().active());

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>("test1")
            .setCacheMode(REPLICATED)
            .setAtomicityMode(TRANSACTIONAL);

        IgniteCache<Object, Object> c1 = ignite.getOrCreateCache(ccfg);

        Pojo pojo = new Pojo(UUID.randomUUID(), "Test");

        try(Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            c1.put(1, 2);
            c1.put(2, pojo);

            tx.commit();
        }

        assertEquals(2, c1.get(1));

        CacheConfiguration<Object, Object> ccfg2 = new CacheConfiguration<>()
            .setName("test2")
            .setCacheMode(REPLICATED);

        IgniteCache<Object, Object> c2 = ignite.getOrCreateCache(ccfg2);

        c2.put(1, 2);

        assertEquals(2, c2.get(1));

        assertTrue(c1.remove(1));

        List<Cache.Entry<Object, Object>> items = c1.query(new ScanQuery<>()).getAll();
        assertEquals(1, items.size());

        Cache.Entry<Object, Object> item = items.get(0);
        assertEquals(2, item.getKey());
        assertEquals(pojo, item.getValue());
    }

    /**
     * Test that isolated node restarts correctly.
     */
    @Test
    public void testRestart() {
        Ignite ignite = Ignition.start(isolatedConfiguration("isolated-node", "data"));

        ignite.cluster().active(true);

        Object consistentId = ignite.cluster().localNode().consistentId();

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache("test-restart");

        cache.put(1, 2);

        stopAllGrids();

        ignite = Ignition.start(isolatedConfiguration("isolated-node", "data"));

        cache = ignite.getOrCreateCache("test-restart");

        assertEquals(2, cache.get(1));
        assertEquals(consistentId, ignite.cluster().localNode().consistentId());
    }

    /**
     * Test that several isolated nodes.
     */
    @Test
    public void testSeveralIsolatedNodes() {
        Ignite ignite1 = Ignition.start(isolatedConfiguration("isolated-node-1", "data-1"));
        ignite1.cluster().active(true);

        try {
            Ignition.start(isolatedConfiguration("isolated-node-2", "data-1"));

            fail("Second isolated node shoud not start on same persistence");
        }
        catch (Throwable e) {
            log.info("Expected exception: " + e.getMessage());
        }

        Ignite ignite2 = Ignition.start(isolatedConfiguration("isolated-node-2", "data-2"));
        ignite2.cluster().active(true);

        assertEquals(1, ignite1.cluster().currentBaselineTopology().size());
        assertEquals(1, ignite2.cluster().currentBaselineTopology().size());
    }

    /**
     * Test that several isolated nodes do not see each other.
     * @throws Exception If failed
     */
    @Test
    public void testIsolatedNodeWithNormalNode() throws Exception {
        Ignite ignite1 = Ignition.start(isolatedConfiguration("isolated-node", "data"));
        ignite1.cluster().active(true);

        Ignite ignite2 = Ignition.start(getConfiguration("normal-node"));

        assertEquals(1, ignite1.cluster().currentBaselineTopology().size());
        assertEquals(1, ignite2.cluster().nodes().size());
    }

    /** Test POJO. */
    private static class Pojo {
        /** */
        private UUID id;

        /** */
        private String name;

        /**
         * @param id ID.
         * @param name Name.
         */
        Pojo(UUID id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * @return ID.
         */
        public UUID getId() {
            return id;
        }

        /**
         * @return Name.
         */
        public String getName() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Pojo pojo = (Pojo)o;

            return Objects.equals(id, pojo.id) &&
                Objects.equals(name, pojo.name);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, name);
        }
    }
}
