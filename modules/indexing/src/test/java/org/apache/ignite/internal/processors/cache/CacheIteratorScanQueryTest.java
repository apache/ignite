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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Node filter test.
 */
public class CacheIteratorScanQueryTest extends GridCommonAbstractTest {
    /** Client mode. */
    private boolean client = false;

    /** Cache configurations. */
    private CacheConfiguration[] ccfgs = null;

    /** */
    public CacheIteratorScanQueryTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        client = false;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setClientMode(client);
        cfg.setCacheConfiguration(ccfgs);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQuery() throws Exception {
        Ignite server = startGrid(0);

        client = true;
        ccfgs = new CacheConfiguration[] {
            new CacheConfiguration("test-cache-replicated").setCacheMode(REPLICATED)
                .setNodeFilter(new AlwaysFalseCacheFilter()),
            new CacheConfiguration("test-cache-partitioned").setCacheMode(PARTITIONED)
                .setNodeFilter(new AlwaysFalseCacheFilter())
        };

        Ignite client = startGrid(1);

        assertEquals(2, server.cluster().nodes().size());
        assertEquals(1, server.cluster().forServers().nodes().size());
        assertEquals(1, server.cluster().forClients().nodes().size());

        assertEquals(2, client.cluster().nodes().size());
        assertEquals(1, client.cluster().forServers().nodes().size());
        assertEquals(1, client.cluster().forClients().nodes().size());

        for (CacheConfiguration cfg : ccfgs) {
            IgniteCache<Object, Object> cache = client.cache(cfg.getName());

            assertNotNull(cache);
            assertNotNull(cache.iterator());
            assertFalse(cache.iterator().hasNext());
        }
    }

    /**
     * Return always false.
     */
    public static class AlwaysFalseCacheFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return false;
        }
    }
}