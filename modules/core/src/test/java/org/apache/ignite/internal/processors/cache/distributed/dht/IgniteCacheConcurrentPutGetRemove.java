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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheConcurrentPutGetRemove extends GridCommonAbstractTest {
    /** */
    private static final int NODES = 4;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutGetRemoveAtomic() throws Exception {
        putGetRemove(cacheConfiguration(ATOMIC, 1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutGetRemoveTx() throws Exception {
        putGetRemove(cacheConfiguration(TRANSACTIONAL, 1));
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void putGetRemove(final CacheConfiguration ccfg) throws Exception {
        ignite(0).createCache(ccfg);

        try {
            long stopTime = System.currentTimeMillis() + 30_000;

            int iter = 0;

            while (System.currentTimeMillis() < stopTime) {
                if (iter++ % 100 == 0)
                    log.info("Iteration: " + iter);

                final AtomicInteger idx = new AtomicInteger();

                final int KEYS = 10;

                GridTestUtils.runMultiThreaded(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        int nodeIdx = idx.getAndIncrement() % NODES;

                        IgniteCache<Object, Object> cache = ignite(nodeIdx).cache(ccfg.getName());

                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        for (int i = 0; i < 10; i++) {
                            for (int k = 0; k < KEYS; k++) {
                                switch (rnd.nextInt(3)) {
                                    case 0:
                                        cache.put(k, rnd.nextInt(10_000));

                                        break;

                                    case 1:
                                        cache.get(k);

                                        break;

                                    case 2:
                                        cache.remove(k);

                                        break;

                                    default:
                                        fail();
                                }
                            }
                        }

                        return null;
                    }
                }, NODES * 10, "update-thread");

                Affinity aff = ignite(0).affinity(ccfg.getName());

                for (int k = 0; k < KEYS; k++) {
                    Collection<ClusterNode> nodes = aff.mapKeyToPrimaryAndBackups(k);

                    Object expVal = grid(nodes.iterator().next()).cache(ccfg.getName()).get(k);

                    for (int n = 0; n < NODES; n++) {
                        Ignite ignite = ignite(n);

                        IgniteCache<Object, Object> cache = ignite.cache(ccfg.getName());

                        if (nodes.contains(ignite.cluster().localNode()))
                            assertEquals(expVal, cache.localPeek(k));
                        else {
                            assertNull(cache.localPeek(k));
                            assertEquals(expVal, cache.get(k));
                        }
                    }
                }
            }
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param backups Backups number.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(CacheAtomicityMode atomicityMode, int backups) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(backups);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }
}
