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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

import static org.apache.ignite.cache.CacheMode.*;

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
        startGrids(2);
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

        assert !nid1.equals(nid2);

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

        assertNull(c1.get("g2"));
        assertNull(c2.get("g1"));
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
