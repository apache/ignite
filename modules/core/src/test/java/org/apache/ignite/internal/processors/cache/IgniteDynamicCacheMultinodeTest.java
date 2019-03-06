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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 *
 */
public class IgniteDynamicCacheMultinodeTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES = 6;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES - 2);

        client = true;

        startGridsMultiThreaded(NODES - 2, 2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetOrCreateCache() throws Exception {
        createCacheMultinode(TestOp.GET_OR_CREATE_CACHE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetOrCreateCaches() throws Exception {
        createCacheMultinode(TestOp.GET_OR_CREATE_CACHES);
    }

    /**
     * @param op Operation to test.
     * @throws Exception If failed.
     */
    private void createCacheMultinode(final TestOp op) throws Exception {
        final int THREADS = NODES * 3;

        for (int i = 0; i < 10; i++) {
            log.info("Iteration: " + i);

            final CyclicBarrier b = new CyclicBarrier(THREADS);

            final AtomicInteger idx = new AtomicInteger();

            final int iter = i;

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    Ignite node = ignite(idx.incrementAndGet() % NODES);

                    b.await();

                    boolean sleep = iter % 2 == 0;

                    if (sleep)
                        Thread.sleep(ThreadLocalRandom.current().nextLong(100) + 1);

                    switch (op) {
                        case GET_OR_CREATE_CACHE:
                            node.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

                            break;

                        case GET_OR_CREATE_CACHES:
                            node.getOrCreateCaches(cacheConfigurations());

                            break;
                    }

                    return null;
                }
            }, THREADS, "start-cache");

            for (String cache : ignite(0).cacheNames())
                ignite(0).destroyCache(cache);

            awaitPartitionMapExchange();
        }
    }

    /**
     * @return Cache configurations.
     */
    private List<CacheConfiguration> cacheConfigurations() {
        List<CacheConfiguration> ccfgs = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            CacheConfiguration ccfg = new CacheConfiguration("cache-" + i);

            ccfg.setAtomicityMode(i % 2 == 0 ? ATOMIC : TRANSACTIONAL);

            ccfgs.add(ccfg);
        }

        return ccfgs;
    }

    /**
     *
     */
    enum TestOp {
        /** */
        GET_OR_CREATE_CACHE,

        /** */
        GET_OR_CREATE_CACHES
    }
}
