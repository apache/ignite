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

import java.util.UUID;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for cache put with error in event listener.
 */
public class CachePutEventListenerErrorSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_EVENTS);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setIncludeEventTypes(EventType.EVT_CACHE_OBJECT_PUT);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_EVENTS);

        startGridsMultiThreaded(3);

        Ignition.setClientMode(true);

        try {
            Ignite ignite = startGrid("client");

            ignite.events().remoteListen(
                new IgniteBiPredicate<UUID, Event>() {
                    @Override public boolean apply(UUID uuid, Event evt) {
                        return true;
                    }
                },
                new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        throw new NoClassDefFoundError("XXX");
                    }
                },
                EventType.EVT_CACHE_OBJECT_PUT
            );
        }
        finally {
            Ignition.setClientMode(false);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionedAtomicOnHeap() throws Exception {
        doTest(CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionedTransactionalOnHeap() throws Exception {
        doTest(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplicatedAtomicOnHeap() throws Exception {
        doTest(CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplicatedTransactionalOnHeap() throws Exception {
        doTest(CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @throws Exception If failed.
     */
    private void doTest(CacheMode cacheMode, CacheAtomicityMode atomicityMode)
        throws Exception {
        Ignite ignite = grid("client");

        try {
            CacheConfiguration<Integer, Integer> cfg = defaultCacheConfiguration();

            cfg.setName("cache");
            cfg.setCacheMode(cacheMode);
            cfg.setAtomicityMode(atomicityMode);

            IgniteCache<Integer, Integer> cache = ignite.createCache(cfg);

            IgniteFuture f = cache.putAsync(0, 0);

            try {
                f.get(2000);

                assert false : "Exception was not thrown";
            }
            catch (CacheException e) {
                info("Caught expected exception: " + e);
            }
        }
        finally {
            ignite.destroyCache("cache");
        }
    }
}
