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

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.testframework.MvccFeatureChecker.assertMvccWriteConflict;

/**
 * Multinode update test.
 */
@SuppressWarnings("unchecked")
@RunWith(JUnit4.class)
public abstract class GridCacheMultinodeUpdateAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    protected static volatile boolean failed;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 3 * 60_000;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        ccfg.setCacheStoreFactory(null);
        ccfg.setReadThrough(false);
        ccfg.setWriteThrough(false);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        failed = false;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvoke() throws Exception {
        if (MvccFeatureChecker.forcedMvcc())
            fail("https://issues.apache.org/jira/browse/IGNITE-10778");

        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        final Integer key = primaryKey(cache);

        cache.put(key, 0);

        final int THREADS = gridCount();
        final int ITERATIONS_PER_THREAD = 1000;

        Integer expVal = 0;

        final long endTime = System.currentTimeMillis() + GridTestUtils.SF.applyLB(60_000, 10_000);

        int iter = 0;

        while (System.currentTimeMillis() < endTime) {
            log.info("Iteration: " + iter++);

            final AtomicInteger gridIdx = new AtomicInteger();

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int idx = gridIdx.incrementAndGet() - 1;

                    final IgniteCache<Integer, Integer> cache = grid(idx).cache(DEFAULT_CACHE_NAME);

                        for (int i = 0; i < ITERATIONS_PER_THREAD && !failed; i++) {
                            boolean updated = false;

                            while (!updated) {
                                try {
                                    cache.invoke(key, new IncProcessor());

                                    updated = true;
                                }
                                catch (Exception e) {
                                    assertMvccWriteConflict(e);
                                }
                            }
                        }

                    return null;
                }
            }, THREADS, "invoke");

            assertFalse("Got null in processor.", failed);

            expVal += ITERATIONS_PER_THREAD * THREADS;

            for (int j = 0; j < gridCount(); j++) {
                Integer val = (Integer)grid(j).cache(DEFAULT_CACHE_NAME).get(key);

                assertEquals("Unexpected value for grid " + j, expVal, val);
            }
        }
    }

    /**
     *
     */
    protected static class IncProcessor implements EntryProcessor<Integer, Integer, Void>, Serializable {
        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Integer> e, Object... args) {
            Integer val = e.getValue();

            if (val == null) {
                failed = true;

                System.out.println(Thread.currentThread() + " got null in processor: " + val);

                return null;
            }

            e.setValue(val + 1);

            return null;
        }
    }
}
