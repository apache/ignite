/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Vacuum test.
 */
public class CacheMvccVacuumTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_BASELINE_AUTO_ADJUST_ENABLED, "false");

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(IGNITE_BASELINE_AUTO_ADJUST_ENABLED);
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartStopVacuumInMemory() throws Exception {
        Ignite node0 = startGrid(0);
        Ignite node1 = startGrid(1);

        node1.createCache(new CacheConfiguration<>("test1")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);

        node1.createCache(new CacheConfiguration<>("test2")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT));

        ensureVacuum(node0);
        ensureVacuum(node1);

        stopGrid(0);

        ensureNoVacuum(node0);
        ensureVacuum(node1);

        stopGrid(1);

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartStopVacuumPersistence() throws Exception {
        persistence = true;

        Ignite node0 = startGrid(0);
        Ignite node1 = startGrid(1);

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);

        node1.cluster().active(true);

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);

        node1.createCache(new CacheConfiguration<>("test1")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);

        IgniteCache<Object, Object> cache = node1.createCache(new CacheConfiguration<>("test2")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT));

        cache.put(primaryKey(cache), 0);
        cache.put(primaryKey(node0.cache("test2")), 0);

        ensureVacuum(node0);
        ensureVacuum(node1);

        node1.cluster().active(false);

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);

        node1.cluster().active(true);

        ensureVacuum(node0);
        ensureVacuum(node1);

        stopGrid(0);

        ensureNoVacuum(node0);
        ensureVacuum(node1);

        stopGrid(1);

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);

        node0 = startGrid(0);
        node1 = startGrid(1);

        node1.cluster().active(true);

        ensureVacuum(node0);
        ensureVacuum(node1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVacuumNotStartedWithoutMvcc() throws Exception {
        IgniteConfiguration cfg = getConfiguration("grid1");

        Ignite node = startGrid(cfg);

        ensureNoVacuum(node);

        IgniteCache<Object, Object> cache = node.createCache(
            cacheConfiguration(PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, 1, 16));

        ensureVacuum(node);

        cache.put(0, 0);

        cache.destroy();

        ensureNoVacuum(node);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8414")
    @Test
    public void testVacuumNotStartedOnNonBaselineNode() throws Exception {
        persistence = true;

        Ignite node0 = startGrid(0);

        ensureNoVacuum(node0);

        node0.cluster().active(true);

        IgniteCache<Object, Object> cache = node0.createCache(
            cacheConfiguration(PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, 1, 16));

        cache.put(1, 0);

        ensureVacuum(node0);

        Ignite node1 = startGrid(1);

        //TODO IGNITE-8414: Test fails here due to cache context initializes on node join unless IGNITE-8414 is fixed.
        ensureNoVacuum(node1);

        node0.cluster().setBaselineTopology(node0.cluster().topologyVersion());

        ensureVacuum(node0);
        ensureVacuum(node1);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8717")
    @Test
    public void testVacuumNotStartedOnNonBaselineNode2() throws Exception {
        persistence = true;

        Ignite node0 = startGrid(0);
        Ignite node1 = startGrid(1);

        node0.cluster().active(true);

        IgniteCache<Object, Object> cache = node0.createCache(
            cacheConfiguration(PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, 1, 16));

        cache.put(primaryKey(cache), 0);
        cache.put(primaryKey(node1.cache(DEFAULT_CACHE_NAME)), 0);

        stopGrid(1);

        node0.cluster().setBaselineTopology(Collections.singleton(node0.cluster().node()));

        //TODO IGNITE-8717: Rejoin node after cache destroy leads critical error unless IGNITE-8717 fixed.
        node0.cache(DEFAULT_CACHE_NAME).destroy();

        ensureNoVacuum(node0);

        node1 = startGrid(1);

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);

        node0.cluster().setBaselineTopology(node0.cluster().topologyVersion());

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVacuumNotStartedOnNonAffinityNode() throws Exception {
        persistence = true;

        Ignite node0 = startGrid(0);
        Ignite node1 = startGrid(1);

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);

        node0.cluster().active(true);

        IgniteCache<Object, Object> cache = node0.createCache(
            cacheConfiguration(PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, 1, 16)
                .setNodeFilter(new NodeFilter(node0.cluster().node().id())));

        cache.put(1, 0);

        ensureVacuum(node0);
        ensureNoVacuum(node1);

        node0.cluster().active(false);

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);

        stopGrid(1);

        ensureNoVacuum(node0);
        ensureNoVacuum(node1);

        node0.cluster().active(true);
        node0.cluster().setBaselineTopology(Collections.singleton(node0.cluster().node()));

        ensureVacuum(node0);
        ensureNoVacuum(node1);

        // Check non-baseline node.
        node1 = startGrid(1);

        ensureVacuum(node0);
        ensureNoVacuum(node1);

        node0.cluster().setBaselineTopology(node0.cluster().topologyVersion());

        ensureVacuum(node0);
        ensureNoVacuum(node1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVacuumNotStartedWithoutMvccPersistence() throws Exception {
        persistence = true;

        IgniteConfiguration cfg = getConfiguration("grid1");

        Ignite node = startGrid(cfg);

        ensureNoVacuum(node);

        node.cluster().active(true);

        ensureNoVacuum(node);
    }

    /**
     * Ensures vacuum is running on the given node.
     *
     * @param node Node.
     */
    private void ensureVacuum(Ignite node) {
        MvccProcessorImpl crd = mvccProcessor(node);

        assertNotNull(crd);

        TxLog txLog = GridTestUtils.getFieldValue(crd, "txLog");

        assertNotNull("TxLog wasn't initialized.", txLog);

        List<GridWorker> vacuumWorkers = GridTestUtils.getFieldValue(crd, "vacuumWorkers");

        assertNotNull("No vacuum workers was initialized.", vacuumWorkers);
        assertFalse("No vacuum workers was initialized.", vacuumWorkers.isEmpty());

        for (GridWorker w : vacuumWorkers) {
            assertFalse(w.isCancelled());
            assertFalse(w.isDone());
        }
    }

    /**
     * Ensures vacuum is stopped on the given node.
     *
     * @param node Node.
     */
    private void ensureNoVacuum(Ignite node) {
        MvccProcessorImpl crd = mvccProcessor(node);

        assertNull("Vacuums workers shouldn't be started.", GridTestUtils.<List<GridWorker>>getFieldValue(crd, "vacuumWorkers"));

        assertNull("TxLog shouldn't exists.", GridTestUtils.getFieldValue(crd, "txLog"));
    }

    /**
     * Filter specifying on which node the cache should be started.
     */
    public static class NodeFilter implements IgnitePredicate<ClusterNode> {
        /** Cache should be created node with certain UUID. */
        public UUID uuid;

        /**
         * @param uuid node ID.
         */
        public NodeFilter(UUID uuid) {
            this.uuid = uuid;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return clusterNode.id().equals(uuid);
        }
    }
}
