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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
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

    /** Used inside InitialQuery listener. */
    private static final CountDownLatch latch = new CountDownLatch(1);

    /** Node where unmarshalling fails with exceptions. */
    private static volatile String failNode;

    /** Used to count UnhandledExceptionEvents at client node. */
    private static final AtomicInteger cnt = new AtomicInteger();

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
        IgniteEx client = grid(0);
        IgniteEx node1 = grid(1);
        IgniteEx node2 = grid(2);

        assert client.configuration().isClientMode() &&
            !node1.configuration().isClientMode() &&
            !node2.configuration().isClientMode();

        failNode = client.name();

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                UnhandledExceptionEvent uex = (UnhandledExceptionEvent)evt;

                assertTrue(X.getFullStackTrace(uex.getException()).
                    contains("IOException: Class can not be unmarshalled"));

                cnt.incrementAndGet();

                return true;
            }
        }, EventType.EVT_UNHANDLED_EXCEPTION);

        node1.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                fail("This line should newer calls.");

                return true;
            }
        }, EventType.EVT_UNHANDLED_EXCEPTION);

        ContinuousQuery<TestKey, String> qry = new ContinuousQuery<>();

        qry.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<TestKey, String>() {
            @Override public boolean apply(TestKey key, String val) {
                latch.countDown(); // Gives guarantee query initialized.

                return true;
            }
        }));

        qry.setLocalListener(new CacheEntryUpdatedListener<TestKey, String>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends TestKey, ? extends String>> evts) {
                fail("This line should newer calls.");
            }
        });

        validate(
            0,//execs
            0,//evts
            0,//fails
            client,
            node1,
            node2);

        // Put element before creating QueryCursor.
        putPrimary(node1);

        try (QueryCursor<Cache.Entry<TestKey, String>> cur = client.cache(null).query(qry)) {
            latch.await();

            validate(
                1,//execs
                0,//evts
                0,//fails
                client,
                node1,
                node2);

            putPrimary(node1);

            validate(
                1,//execs
                1,//evts
                1,//fails
                client,
                node1,
                node2);

            putPrimary(node2);

            validate(
                1,//execs
                2,//evts
                2,//fails
                client,
                node1,
                node2);
        }
    }

    /**
     * @param ignite Ignite.
     */
    private void putPrimary(IgniteEx ignite) {
        IgniteCache<TestKey, Object> cache = ignite.cache(null);

        cache.put(generateNodeKeys(ignite, cache), "value");
    }

    /**
     * @param execs Executions.
     * @param evts Events.
     * @param failsNum Fails number.
     * @param client Client.
     * @param node1 Node 1.
     * @param node2 Node 2.
     */
    private void validate(final int execs, final int evts, final int failsNum, final IgniteEx client, IgniteEx node1,
        IgniteEx node2) throws Exception {
        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return client.cache(null).queryMetrics().fails() == failsNum;
            }
        }, 5_000));

        assertEquals(evts, cnt.intValue());

        validateCacheQueryMetrics(client, execs, failsNum);
        validateCacheQueryMetrics(node1, 0, 0);
        validateCacheQueryMetrics(node2, 0, 0);
    }

    /**
     * @param ignite Ignite.
     * @param executions Executions.
     * @param fails Fails.
     */
    private void validateCacheQueryMetrics(IgniteEx ignite, int executions, int fails) {
        IgniteCache<Object, Object> cache = ignite.cache(null);

        GridCacheQueryMetricsAdapter metr = (GridCacheQueryMetricsAdapter)cache.queryMetrics();

        assertEquals(metr.executions(), executions);

        assertEquals(metr.fails(), fails);
    }

    /**
     * @param node Node.
     * @param cache Cache.
     */
    private TestKey generateNodeKeys(IgniteEx node, IgniteCache<TestKey, Object> cache) {

        ClusterNode locNode = node.localNode();

        for (int ind = 0; ind < 100_000; ind++) {
            TestKey key = new TestKey("key" + ind);

            if (affinity(cache).isPrimary(locNode, key))
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

            IgniteCacheP2pUnmarshallingContinuousQueryErrorTest.TestKey key = (IgniteCacheP2pUnmarshallingContinuousQueryErrorTest.TestKey)o;

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
            field = (String)in.readObject();

            if (((IgniteThread)Thread.currentThread()).getGridName().equals(failNode))
                throw new IOException("Class can not be unmarshalled.");

        }
    }
}
