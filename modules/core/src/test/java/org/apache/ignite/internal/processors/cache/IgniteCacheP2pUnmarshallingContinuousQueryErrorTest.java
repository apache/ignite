/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.UnhandledExceptionEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMetricsAdapter;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.thread.IgniteThread;

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Checks behavior on exception while unmarshalling key for continuous query.
 */
public class IgniteCacheP2pUnmarshallingContinuousQueryErrorTest extends IgniteCacheP2pUnmarshallingErrorTest {
    /**
     * {@inheritDoc}
     */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * Used inside InitialQuery listener
     */
    private static CountDownLatch latchInitQry = new CountDownLatch(1);

    /**
     * Node for fail.
     */
    private static String node4fail;

    /**
     * {@inheritDoc}
     */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cacheCfg = super.cacheConfiguration(gridName);

        cacheCfg.setStatisticsEnabled(true);

        return cacheCfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void testResponseMessageOnUnmarshallingFailed() throws Exception {
        final AtomicInteger unhandledECntr = new AtomicInteger();

        IgniteEx cNode = grid(0);

        assert cNode.configuration().isClientMode();

        node4fail = cNode.name();

        IgniteEx pNode = grid(1);
        IgniteEx bBode = grid(2);

        assert !pNode.configuration().isClientMode();
        assert !pNode.configuration().isClientMode();

        IgniteCache<Object, Object> cCache = jcache(0);
        IgniteCache<Object, Object> pCache = jcache(1);
        IgniteCache<Object, Object> bCache = jcache(2);

        cNode.events().localListen(new IgnitePredicate<Event>() {
            @Override
            public boolean apply(Event evt) {
                UnhandledExceptionEvent uex = (UnhandledExceptionEvent) evt;

                String eMsg = X.getFullStackTrace(uex.getException());

                assertTrue(eMsg.contains("IOException: Class can not be unmarshalled"));

                unhandledECntr.incrementAndGet();

                return true;
            }
        }, EventType.EVT_UNHANDLED_EXCEPTION);

        pNode.events().localListen(new IgnitePredicate<Event>() {
            @Override
            public boolean apply(Event evt) {
                fail("This line newer calls");
                return true;
            }
        }, EventType.EVT_UNHANDLED_EXCEPTION);

        ContinuousQuery<TestKey, String> qry = new ContinuousQuery<>();

        qry.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<TestKey, String>() {
            @Override
            public boolean apply(TestKey key, String val) {
                latchInitQry.countDown();
                return true;
            }
        }));

        qry.setLocalListener(new CacheEntryUpdatedListener<TestKey, String>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends TestKey, ? extends String>> evts) {
                fail("This line newer calls");
            }
        });

        // Before test
        validateCacheQueryMetrics(cCache, 0, 0);
        validateCacheQueryMetrics(pCache, 0, 0);
        validateCacheQueryMetrics(bCache, 0, 0);

        assertEquals(unhandledECntr.intValue(), 0);

        // Put element before creating QueryCursor.
        pCache.put(generateNodeKeys(pNode, pCache), "value");

        try (QueryCursor<Cache.Entry<TestKey, String>> cur = jcache(0).query(qry)) {
            latchInitQry.await();

            jcache(1).clear();

            pCache.put(generateNodeKeys(pNode, pCache), "value");

            Validate(1);

            assertEquals(1, unhandledECntr.intValue());

            bCache.put(generateNodeKeys(bBode, bCache), "value");

            Validate(2);

            assertEquals(2, unhandledECntr.intValue());
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void Validate(final int failsNum) throws Exception {

        boolean res = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override
            public boolean apply() {
                return jcache(0).queryMetrics().fails() == failsNum;
            }
        }, 5_000);

        assertTrue(res);

        validateCacheQueryMetrics(jcache(0), 1, failsNum);
        validateCacheQueryMetrics(jcache(1), 0, 0);
        validateCacheQueryMetrics(jcache(2), 0, 0);
    }

    /**
     * @throws Exception If failed.
     */
    private void validateCacheQueryMetrics(IgniteCache cache, int executions, int faild) {
        GridCacheQueryMetricsAdapter metr = (GridCacheQueryMetricsAdapter) cache.queryMetrics();

        log.info(metr.toString());

        assertEquals(metr.executions(), executions);

        assertEquals(metr.fails(), faild);
    }


    /**
     * @param node  Node.
     * @param cache Cache.
     */
    private TestKey generateNodeKeys(IgniteEx node, IgniteCache cache) {

        ClusterNode locNode = node.localNode();

        Affinity<TestKey> aff = (Affinity<TestKey>) affinity(cache);

        for (int ind = 0; ind < 100_000; ind++) {
            TestKey key = new TestKey("key" + ind);

            if (aff.isPrimary(locNode, key))
                return key;
        }

        throw new IgniteException("Unable to find key keys as primary for cache.");
    }

    /**
     *
     * */
    private static class TestKey implements Externalizable {
        /**
         * Field.
         */
        @QuerySqlField(index = true)
        private String field;

        /**
         * Required by {@link Externalizable}.
         */
        public TestKey() {
        }

        /**
         * @param field Test key 1.
         */
        public TestKey(String field) {
            this.field = field;
        }

        /**
         * {@inheritDoc}
         */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            IgniteCacheP2pUnmarshallingContinuousQueryErrorTest.TestKey key = (IgniteCacheP2pUnmarshallingContinuousQueryErrorTest.TestKey) o;

            return !(field != null ? !field.equals(key.field) : key.field != null);
        }

        /**
         * {@inheritDoc}
         */
        @Override public int hashCode() {
            return field != null ? field.hashCode() : 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(field);
        }

        /**
         * {@inheritDoc}
         */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            field = (String) in.readObject();

            if (((IgniteThread) Thread.currentThread()).getGridName().equals(node4fail))
                throw new IOException("Class can not be unmarshalled.");

        }
    }
}
