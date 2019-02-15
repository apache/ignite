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

package org.apache.ignite.internal.processors.cache.eviction.sorted;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * {@link SortedEvictionPolicy} performance test.
 */
public class SortedEvictionPolicyPerformanceTest extends GridCommonAbstractTest {
    /** Threads. */
    private static final int THREADS = 8;

    /** Keys. */
    private static final int KEYS = 100_000;

    /** Max size. */
    private static final int MAX_SIZE = 1000;

    /** Put probability. */
    private static final int P_PUT = 50;

    /** Get probability. */
    private static final int P_GET = 30;

    /** Rnd. */
    private static final ThreadLocalRandom RND = ThreadLocalRandom.current();

    /** Ignite. */
    private static Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        ignite = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setNearConfiguration(null);

        SortedEvictionPolicy plc = new SortedEvictionPolicy();
        plc.setMaxSize(MAX_SIZE);

        ccfg.setEvictionPolicy(plc);
        ccfg.setOnheapCacheEnabled(true);

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Tests throughput.
     */
    @Test
    public void testThroughput() throws Exception {
        final LongAdder cnt = new LongAdder();
        final AtomicBoolean finished = new AtomicBoolean();

        final int pPut = P_PUT;
        final int pGet = P_PUT + P_GET;

        final IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (;;) {
                    U.sleep(1000);

                    info("Ops/sec: " + cnt.sumThenReset());
                }
            }
        }, 1);

        multithreaded(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    while (!finished.get()) {
                        int p = RND.nextInt(100);

                        int key = RND.nextInt(KEYS);

                        if (p >= 0 && p < pPut)
                            cache.put(key, 0);
                        else if (p >= pPut && p < pGet)
                            cache.get(key);
                        else
                            cache.remove(key);

                        cnt.increment();
                    }

                    return null;
                }
            }, THREADS);
    }
}
