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

package org.apache.ignite.internal.processors.client;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CachePeekMode.ALL;

/**
 */
@RunWith(JUnit4.class)
public class IgniteDataStreamerTest extends GridCommonAbstractTest {
    public static final String CACHE_NAME = "UUID_CACHE";

    public static final int DATA_SIZE = 3;

    public static final long WAIT_TIMEOUT = 30_000L;

    private boolean client = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (client)
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);

        client = true;

        startGrid("client");
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        grid("client").destroyCache(CACHE_NAME);
    }

    /**
     * @return Cache configuration
     */
    private <K, V> CacheConfiguration<K, V> cacheConfiguration(Class<K> key, Class<V> value) {
        CacheConfiguration<K, V> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setIndexedTypes(key, value);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStreamerIgniteUuid() throws Exception {
        Ignite client = grid("client");

        IgniteCache<IgniteUuid, Integer> cache =
            client.createCache(cacheConfiguration(IgniteUuid.class, Integer.class));

        try(IgniteDataStreamer<IgniteUuid, Integer> streamer = client.dataStreamer(CACHE_NAME)) {
            assertTrue("Expecting " + DataStreamerImpl.class.getName(), streamer instanceof DataStreamerImpl);

            ((DataStreamerImpl<IgniteUuid, Integer>)streamer).maxRemapCount(0);

            List<IgniteFuture> futs = new ArrayList<>();

            for(int i=0; i<DATA_SIZE; i++) {
                IgniteFuture<?> fut = streamer.addData(IgniteUuid.randomUuid(), i);

                futs.add(fut);
            }

            streamer.flush();

            for (IgniteFuture fut : futs) {
                //This should not throw any exception.
                Object res = fut.get(WAIT_TIMEOUT);

                //Printing future result to log to prevent jvm optimization
                log.debug(res == null ? null : res.toString());
            }

            assertTrue(cache.size(ALL) == DATA_SIZE);
        }
    }
}
