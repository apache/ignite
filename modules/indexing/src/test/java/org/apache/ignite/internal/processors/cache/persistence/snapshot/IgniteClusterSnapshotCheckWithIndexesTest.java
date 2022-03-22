/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Cluster-wide snapshot test check command with indexes.
 */
public class IgniteClusterSnapshotCheckWithIndexesTest extends AbstractSnapshotSelfTest {
    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckEmptyCache() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, 0, key -> new Account(key, key),
            txFilteredCache("indexed"));

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        IdleVerifyResultV2 res = ignite.context().cache().context().snapshotMgr().checkSnapshot(SNAPSHOT_NAME).get();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertTrue("Exceptions: " + b, F.isEmpty(res.exceptions()));
        assertTrue(F.isEmpty(res.exceptions()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckWithIndexes() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, CACHE_KEYS_RANGE, key -> new Account(key, key),
            txFilteredCache("indexed"));

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        IdleVerifyResultV2 res = ignite.context().cache().context().snapshotMgr().checkSnapshot(SNAPSHOT_NAME).get();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertTrue("Exceptions: " + b, F.isEmpty(res.exceptions()));
        assertContains(log, b.toString(), "The check procedure has finished, no conflicts have been found.");
    }

    /** @throws Exception If failed. */
    @Test
    public void testClusterSnapshotCheckWithNodeFilter() throws Exception {
        startGridsWithoutCache(2);

        IgniteCache<Integer, Account> cache1 = grid(0).createCache(txFilteredCache("cache0")
            .setNodeFilter(new SelfNodeFilter(grid(0).localNode().id())));
        IgniteCache<Integer, Account> cache2 = grid(1).createCache(txFilteredCache("cache1")
            .setNodeFilter(new SelfNodeFilter(grid(1).localNode().id())));

        for (int i = 0; i < CACHE_KEYS_RANGE; i++) {
            cache1.put(i, new Account(i, i));
            cache2.put(i, new Account(i, i));
        }

        grid(0).snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        IdleVerifyResultV2 res = grid(0).context().cache().context().snapshotMgr().checkSnapshot(SNAPSHOT_NAME).get();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertTrue("Exceptions: " + b, F.isEmpty(res.exceptions()));
        assertContains(log, b.toString(), "The check procedure has finished, no conflicts have been found.");
    }

    /** Node filter to run cache on single node. */
    private static class SelfNodeFilter implements IgnitePredicate<ClusterNode> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Node id to run cache at. */
        private final UUID nodeId;

        /** @param nodeId Node id to run cache at. */
        public SelfNodeFilter(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return node.id().equals(nodeId);
        }
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Account> txFilteredCache(String cacheName) {
        return txCacheConfig(new CacheConfiguration<Integer, Account>(cacheName))
            .setCacheMode(CacheMode.REPLICATED)
            .setQueryEntities(singletonList(new QueryEntity(Integer.class.getName(), Account.class.getName())));
    }
}
