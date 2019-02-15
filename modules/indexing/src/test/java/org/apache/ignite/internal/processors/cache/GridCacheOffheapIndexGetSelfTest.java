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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;

/**
 * Tests off heap storage when both offheaped and swapped entries exists.
 */
@RunWith(JUnit4.class)
public class GridCacheOffheapIndexGetSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setNetworkTimeout(2000);

        cfg.setDeploymentMode(SHARED);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setEvictionPolicy(null);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for(String cacheName : grid(0).cacheNames()) {
            info("Clear cache: " + cacheName);

            grid(0).cache(cacheName).clear();
        }
    }

    /**
     * Tests behavior on offheaped entries.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGet() throws Exception {
        IgniteCache<Long, Long> cache = jcache(grid(0), cacheConfiguration(), Long.class, Long.class);

        for (long i = 0; i < 100; i++)
            cache.put(i, i);

        for (long i = 0; i < 100; i++)
            assertEquals((Long)i, cache.get(i));

        SqlQuery<Long, Long> qry = new SqlQuery<>(Long.class, "_val >= 90");

        List<Cache.Entry<Long, Long>> res = cache.query(qry).getAll();

        assertEquals(10, res.size());

        for (Cache.Entry<Long, Long> e : res) {
            assertNotNull(e.getKey());
            assertNotNull(e.getValue());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutGet() throws Exception {
        IgniteCache<Object, Object> cache = jcache(grid(0), cacheConfiguration(), Object.class, Object.class);

        Map map = new HashMap();

        try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.PESSIMISTIC,
            TransactionIsolation.REPEATABLE_READ, 100000, 1000)) {

            for (int i = 4; i < 400; i++) {
                map.put("key" + i, new TestEntity("value"));
                map.put(i, "value");
            }

            cache.putAll(map);

            tx.commit();
        }

        for (int i = 0; i < 100; i++) {
            cache.get("key" + i);
            cache.get(i);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWithExpiryPolicy() throws Exception {
        IgniteCache<Long, Long> cache = jcache(grid(0), cacheConfiguration(), Long.class, Long.class);

        cache = cache.withExpiryPolicy(new TestExiryPolicy());

        for (long i = 0; i < 100; i++)
            cache.put(i, i);

        for (long i = 0; i < 100; i++)
            assertEquals((Long)i, cache.get(i));

        SqlQuery<Long, Long> qry = new SqlQuery<>(Long.class, "_val >= 90");

        List<Cache.Entry<Long, Long>> res = cache.query(qry).getAll();

        assertEquals(10, res.size());

        for (Cache.Entry<Long, Long> e : res) {
            assertNotNull(e.getKey());
            assertNotNull(e.getValue());
        }
    }

    /**
     *
     */
    private static class TestExiryPolicy implements ExpiryPolicy {
        /** {@inheritDoc} */
        @Override public Duration getExpiryForCreation() {
            return Duration.ONE_MINUTE;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForAccess() {
            return Duration.FIVE_MINUTES;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForUpdate() {
            return Duration.TWENTY_MINUTES;
        }
    }

    /**
     * Test entry class.
     */
    private static class TestEntity implements Serializable {
        /** Value. */
        @QuerySqlField(index = true)
        private String val;

        /**
         * @param value Value.
         */
        public TestEntity(String value) {
            this.val = value;
        }

        /**
         * @return Value.
         */
        public String getValue() {
            return val;
        }

        /**
         * @param val Value
         */
        public void setValue(String val) {
            this.val = val;
        }
    }
}
