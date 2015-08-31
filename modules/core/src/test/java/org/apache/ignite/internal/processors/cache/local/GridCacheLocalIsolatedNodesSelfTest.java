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

package org.apache.ignite.internal.processors.cache.local;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Isolated nodes LOCAL cache self test.
 */
public class GridCacheLocalIsolatedNodesSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public GridCacheLocalIsolatedNodesSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *
     * @throws Exception If test failed.
     */
    public void testIsolatedNodes() throws Exception {
        Ignite g1 = grid(0);
        UUID nid1 = g1.cluster().localNode().id();

        Ignite g2 = grid(1);
        UUID nid2 = g2.cluster().localNode().id();

        Ignite g3 = grid(2);
        UUID nid3 = g3.cluster().localNode().id();

        assert !nid1.equals(nid2);
        assert !nid1.equals(nid3);

        // Local cache on first node only.
        CacheConfiguration<String, String> ccfg1 = new CacheConfiguration<>("A");
        ccfg1.setCacheMode(LOCAL);
        ccfg1.setNodeFilter(new NodeIdFilter(nid1));

        IgniteCache<String, String> c1 = g1.createCache(ccfg1);
        c1.put("g1", "c1");

        // Local cache on second node only.
        CacheConfiguration<String, String> ccfg2 = new CacheConfiguration<>("A");
        ccfg2.setCacheMode(LOCAL);
        ccfg2.setNodeFilter(new NodeIdFilter(nid2));

        IgniteCache<String, String> c2 = g2.createCache(ccfg2);
        c2.put("g2", "c2");

        // Local cache on third node only.
        CacheConfiguration<String, String> ccfg3 = new CacheConfiguration<>("A");
        ccfg3.setCacheMode(LOCAL);
        ccfg3.setNodeFilter(new NodeIdFilter(nid3));

        IgniteCache<String, String> c3 = g3.createCache(ccfg3);
        c3.put("g3", "c3");

        assertNull(c1.get("g2"));
        assertNull(c1.get("g3"));
        assertNull(c2.get("g1"));
        assertNull(c2.get("g3"));
        assertNull(c3.get("g1"));
        assertNull(c3.get("g2"));
    }

    /** Filter by node ID. */
    private static class NodeIdFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private final UUID nid;

        /**
         * @param nid Node ID where cache should be started.
         */
        NodeIdFilter(UUID nid) {
            this.nid = nid;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode n) {
            return n.id().equals(nid);
        }
    }

}