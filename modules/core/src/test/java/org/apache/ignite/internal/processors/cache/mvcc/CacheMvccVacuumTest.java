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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiTuple;
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

        runVacuumSync();
        checkOldVersions();
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
     * Checks if outdated versions were cleaned after the vacuum process.
     *
     * @return {@code False} if not cleaned.
     * @throws IgniteCheckedException If failed.
     */
    private void checkOldVersions() throws IgniteCheckedException {
        for (Ignite node : G.allGrids()) {
            for (IgniteCacheProxy cache : ((IgniteKernal)node).caches()) {
                GridCacheContext cctx = cache.context();

                if (!cctx.userCache() || !cctx.group().mvccEnabled() || F.isEmpty(cctx.group().caches()) || cctx.shared().closed(cctx))
                    continue;

                try (GridCloseableIterator it = (GridCloseableIterator)cache.withKeepBinary().iterator()) {
                    while (it.hasNext()) {
                        IgniteBiTuple entry = (IgniteBiTuple)it.next();

                        KeyCacheObject key = cctx.toCacheKeyObject(entry.getKey());

                        List<IgniteBiTuple<Object, MvccVersion>> vers = cctx.offheap().mvccAllVersions(cctx, key)
                            .stream().filter(t -> t.get1() != null).collect(Collectors.toList());

                        if (vers.size() > 1) {
                            fail("[key=" + key.value(null, false) + "; vers=" + vers + ']');
                        }
                    }
                }
            }
        }
    }


    /**
     * Runs vacuum on all nodes and waits for its completion.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void runVacuumSync() throws IgniteCheckedException {
        GridCompoundIdentityFuture<VacuumMetrics> fut = new GridCompoundIdentityFuture<>();

        // Run vacuum manually.
        for (Ignite node : G.allGrids()) {
            if (!node.configuration().isClientMode()) {
                MvccProcessorImpl crd = mvccProcessor(node);

                if (!crd.mvccEnabled() || GridTestUtils.getFieldValue(crd, "vacuumWorkers") == null)
                    continue;

                assert GridTestUtils.getFieldValue(crd, "txLog") != null;

                fut.add(crd.runVacuum());
            }
        }

        fut.markInitialized();

        // Wait vacuum finished.
        fut.get(getTestTimeout());
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
