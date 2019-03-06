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

import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheScanPredicateDeploymentSelfTest extends GridCommonAbstractTest {
    /** Test value. */
    protected static final String TEST_PREDICATE = "org.apache.ignite.tests.p2p.CacheDeploymentAlwaysTruePredicate";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceName(3).equals(igniteInstanceName))
            cfg.setClassLoader(getExternalClassLoader());

        cfg.setCacheConfiguration(cacheConfiguration());

        cfg.setConnectorConfiguration(null);

        return cfg;
    }

    /**
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    protected CacheConfiguration cacheConfiguration() throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setRebalanceMode(SYNC);
        cfg.setAtomicityMode(atomicityMode());
        cfg.setBackups(1);

        return cfg;
    }

    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testDeployScanPredicate() throws Exception {
        startGrids(4);

        awaitPartitionMapExchange();

        try {
            IgniteCache<Object, Object> cache = grid(3).cache(DEFAULT_CACHE_NAME);

            // It is important that there are no too many keys.
            for (int i = 0; i < 1; i++)
                cache.put(i, i);

            Class predCls = grid(3).configuration().getClassLoader().loadClass(TEST_PREDICATE);

            IgniteBiPredicate<Object, Object> pred = (IgniteBiPredicate<Object, Object>)predCls.newInstance();

            List<Cache.Entry<Object, Object>> all = cache.query(new ScanQuery<>(pred)).getAll();

            assertEquals(1, all.size());
        }
        finally {
            stopAllGrids();
        }
    }
}
