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

import java.util.Random;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests cache get/put/remove and data streaming in read-only cluster mode.
 */
@RunWith(JUnit4.class)
public class ClusterReadOnlyModeTest extends ClusterReadOnlyModeAbstractTest {
    /**
     * Tests cache get/put/remove.
     */
    @Test
    public void testCacheGetPutRemove() {
        assertCachesReadOnlyMode(false);

        changeClusterReadOnlyMode(true);

        assertCachesReadOnlyMode(true);

        changeClusterReadOnlyMode(false);

        assertCachesReadOnlyMode(false);
    }

    /**
     * Tests data streamer.
     */
    @Test
    public void testDataStreamerReadOnly() {
        assertDataStreamerReadOnlyMode(false);

        changeClusterReadOnlyMode(true);

        assertDataStreamerReadOnlyMode(true);

        changeClusterReadOnlyMode(false);

        assertDataStreamerReadOnlyMode(false);
    }

    /**
     * Asserts that all caches in read-only or in read/write mode on all nodes.
     *
     * @param readOnly If {@code true} then cache must be in read only mode, else in read/write mode.
     */
    private void assertCachesReadOnlyMode(boolean readOnly) {
        Random rnd = new Random();

        for (Ignite ignite : G.allGrids()) {
            for (String cacheName : CACHE_NAMES) {
                IgniteCache<Integer, Integer> cache = ignite.cache(cacheName);

                for (int i = 0; i < 10; i++) {
                    cache.get(rnd.nextInt(100)); // All gets must succeed.

                    if (readOnly) {
                        // All puts must fail.
                        try {
                            cache.put(rnd.nextInt(100), rnd.nextInt());

                            fail("Put must fail for cache " + cacheName);
                        }
                        catch (Exception e) {
                            // No-op.
                        }

                        // All removes must fail.
                        try {
                            cache.remove(rnd.nextInt(100));

                            fail("Remove must fail for cache " + cacheName);
                        }
                        catch (Exception e) {
                            // No-op.
                        }
                    }
                    else {
                        cache.put(rnd.nextInt(100), rnd.nextInt()); // All puts must succeed.

                        cache.remove(rnd.nextInt(100)); // All removes must succeed.
                    }
                }
            }
        }
    }

    /**
     * @param readOnly If {@code true} then data streamer must fail, else succeed.
     */
    private void assertDataStreamerReadOnlyMode(boolean readOnly) {
        Random rnd = new Random();

        for (Ignite ignite : G.allGrids()) {
            for (String cacheName : CACHE_NAMES) {
                boolean failed = false;

                try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(cacheName)) {
                    for (int i = 0; i < 10; i++) {
                        ((DataStreamerImpl)streamer).maxRemapCount(5);

                        streamer.addData(rnd.nextInt(1000), rnd.nextInt());
                    }
                }
                catch (CacheException ignored) {
                    failed = true;
                }

                if (failed != readOnly)
                    fail("Streaming to " + cacheName + " must " + (readOnly ? "fail" : "succeed"));
            }
        }
    }
}
