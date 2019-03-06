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
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgniteCacheSerializationSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES = 3;

    /** */
    private static final int CLIENT = NODES - 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceName(CLIENT).equals(igniteInstanceName))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODES);
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(CacheMode cacheMode, CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(atomicityMode);

        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSerializeClosure() throws Exception {
        Ignite client = ignite(CLIENT);

        final IgniteCache<Integer, Integer> clientCache = client.createCache(cacheConfiguration(PARTITIONED, ATOMIC));

        try {
            client.compute(client.cluster().forRemotes().forRandom()).call(new IgniteCallable<Object>() {
                @Override public Object call() throws Exception {
                    clientCache.withKeepBinary();
                    clientCache.withSkipStore();

                    return null;
                }
            });
        }
        finally {
            client.destroyCache(DEFAULT_CACHE_NAME);
        }
    }
}
