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

import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 *
 */
public class CacheQueryFilterExpiredTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFilterExpired() throws Exception {
        try (Ignite ignite = startGrid(0)) {
            checkFilterExpired(ignite, ATOMIC, false);

            checkFilterExpired(ignite, ATOMIC, true);

            checkFilterExpired(ignite, TRANSACTIONAL, false);

            checkFilterExpired(ignite, TRANSACTIONAL, true);
        }
    }

    /**
     * @param ignite Node.
     * @param atomicityMode Cache atomicity mode.
     * @param eagerTtl Value for {@link CacheConfiguration#setEagerTtl(boolean)}.
     * @throws Exception If failed.
     */
    private void checkFilterExpired(Ignite ignite, CacheAtomicityMode atomicityMode, boolean eagerTtl) throws Exception {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setEagerTtl(eagerTtl);
        ccfg.setIndexedTypes(Integer.class, Integer.class);

        final IgniteCache<Integer, Integer> cache = ignite.createCache(ccfg);

        try {
            IgniteCache<Integer, Integer> expCache =
                cache.withExpiryPolicy(new TouchedExpiryPolicy(new Duration(0, 2000)));

            for (int i = 0; i < 10; i++) {
                IgniteCache<Integer, Integer> cache0 = i % 2 == 0 ? cache : expCache;

                cache0.put(i, i);
            }

            assertEquals(10, cache.query(new SqlQuery<Integer, Integer>(Integer.class, "1=1")).getAll().size());
            assertEquals(10, cache.query(new SqlFieldsQuery("select _key, _val from Integer")).getAll().size());

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return cache.query(new SqlQuery<Integer, Integer>(Integer.class, "1=1")).getAll().size() == 5 &&
                        cache.query(new SqlFieldsQuery("select _key, _val from Integer")).getAll().size() == 5;
                }
            }, 5000);

            assertEquals(5, cache.query(new SqlQuery<Integer, Integer>(Integer.class, "1=1")).getAll().size());
            assertEquals(5, cache.query(new SqlFieldsQuery("select _key, _val from Integer")).getAll().size());
        }
        finally {
            ignite.destroyCache(ccfg.getName());
        }
    }
}
