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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.cache.Cache;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

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
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
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
    @Test
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
     * @throws Exception If failed.
     */
    @Test
    public void testQueryGetAllClientSide() throws Exception {
        Ignite server = startGrid(0);

        IgniteCache<Integer, Integer> cache = server.getOrCreateCache(DEFAULT_CACHE_NAME);

        client = true;

        Ignite client = startGrid(1);

        IgniteCache<Integer, Integer> cliCache = client.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100_000; i++)
            cache.put(i, i);

        ScanQuery<Integer, Integer> qry = new ScanQuery<>();

        qry.setPageSize(100);

        try (QueryCursor<Cache.Entry<Integer, Integer>> cur = cliCache.query(qry)) {
            List<Cache.Entry<Integer, Integer>> res = cur.getAll();

            assertEquals(100_000, res.size());

            Collections.sort(res, (e1, e2) -> {
                    return e1.getKey().compareTo(e2.getKey());
            });

            int exp = 0;

            for (Cache.Entry<Integer, Integer> e : res) {
                assertEquals(exp, e.getKey().intValue());
                assertEquals(exp, e.getValue().intValue());

                exp++;
            }
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
