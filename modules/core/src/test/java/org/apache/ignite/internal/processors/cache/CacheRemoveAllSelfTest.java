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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.apache.ignite.testframework.MvccFeatureChecker;

/**
 * Test remove all method.
 */
@RunWith(JUnit4.class)
public class CacheRemoveAllSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        if (MvccFeatureChecker.forcedMvcc())
            fail("https://issues.apache.org/jira/browse/IGNITE-10082");

        super.setUp();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 2 * 60 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAll() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 10_000; ++i)
            cache.put(i, "val");

        final AtomicInteger igniteId = new AtomicInteger(gridCount());

        IgniteInternalFuture fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < 2; ++i)
                    startGrid(igniteId.getAndIncrement());

                return true;
            }
        }, 3, "start-node-thread");

        cache.removeAll();

        fut.get();

        U.sleep(5000);

        for (int i = 0; i < igniteId.get(); ++i) {
            IgniteCache locCache = grid(i).cache(DEFAULT_CACHE_NAME);

            assertEquals("Local size: " + locCache.localSize() + "\n" +
                "On heap: " + locCache.localSize(CachePeekMode.ONHEAP) + "\n" +
                "Off heap: " + locCache.localSize(CachePeekMode.OFFHEAP) + "\n" +
                "Primary: " + locCache.localSize(CachePeekMode.PRIMARY) + "\n" +
                "Backup: " + locCache.localSize(CachePeekMode.BACKUP),
                0, locCache.localSize());
        }
    }
}
