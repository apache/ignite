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

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Verifies that containsKey() works as expected on atomic cache.
 */
public class IgniteCacheContainsKeyAtomicTest extends GridCacheAbstractSelfTest {
    /** Cache name. */
    public static final String CACHE_NAME = "replicated";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        IgniteCache cache = ignite(0).cache(CACHE_NAME);

        if (cache != null)
            cache.clear();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContainsPutIfAbsent() throws Exception {
        checkPutIfAbsent(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContainsPutIfAbsentAll() throws Exception {
        checkPutIfAbsent(true);
    }

    /**
     * @param all Check for set of keys.
     * @throws Exception If failed.
     */
    private void checkPutIfAbsent(final boolean all) throws Exception {
        Ignite srv = ignite(0);

        final IgniteCache<Integer, Integer> cache1 = srv.getOrCreateCache(replicatedCache());
        final IgniteCache<Integer, Integer> cache2 = ignite(1).getOrCreateCache(replicatedCache());

        final AtomicInteger fails = new AtomicInteger(0);

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < 100; i++) {
                    if (!cache1.putIfAbsent(i, i)) {
                        if (all ? !cache2.containsKeys(Collections.singleton(i)) : !cache2.containsKey(i))
                            fails.incrementAndGet();
                    }
                }
            }
        }, 100, "put-if-abs");

        assertEquals(0, fails.get());
    }

    /**
     * @return replicated cache configuration.
     */
    private CacheConfiguration<Integer, Integer> replicatedCache() {
        return new CacheConfiguration<Integer, Integer>(CACHE_NAME)
            .setAtomicityMode(ATOMIC)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setReadFromBackup(false) // containsKey() must respect this flag
            .setCacheMode(REPLICATED);
    }
}
