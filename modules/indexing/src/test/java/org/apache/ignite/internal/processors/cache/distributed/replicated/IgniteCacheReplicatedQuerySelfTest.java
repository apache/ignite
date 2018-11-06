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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractQuerySelfTest;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testsuites.IgniteIgnore;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CachePeekMode.ALL;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Tests replicated query.
 */
public class IgniteCacheReplicatedQuerySelfTest extends IgniteCacheAbstractQuerySelfTest {
    /** */
    private static final boolean TEST_DEBUG = false;

    /** Grid1. */
    private static Ignite ignite1;

    /** Grid2. */
    private static Ignite ignite2;

    /** Grid3. */
    private static Ignite ignite3;

    /** Cache1. */
    private static IgniteCache<CacheKey, CacheValue> cache1;

    /** Cache2. */
    private static IgniteCache<CacheKey, CacheValue> cache2;

    /** Cache3. */
    private static IgniteCache<CacheKey, CacheValue> cache3;

    /** Key serialization cnt. */
    private static volatile int keySerCnt;

    /** Key deserialization count. */
    private static volatile int keyDesCnt;

    /** Value serialization count. */
    private static volatile int valSerCnt;

    /** Value deserialization count. */
    private static volatile int valDesCnt;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite1 = grid(0);
        ignite2 = grid(1);
        ignite3 = grid(2);

        cache1 = jcache(ignite1, CacheKey.class, CacheValue.class);
        cache2 = jcache(ignite2, CacheKey.class, CacheValue.class);
        cache3 = jcache(ignite3, CacheKey.class, CacheValue.class);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        ignite1 = null;
        ignite2 = null;
        ignite3 = null;

        cache1 = null;
        cache2 = null;
        cache3 = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientOnlyNode() throws Exception {
        try {
            Ignite g = startGrid("client");

            IgniteCache<Integer, Integer> c = jcache(g, Integer.class, Integer.class);

            for (int i = 0; i < 10; i++)
                c.put(i, i);

            // Client cache should be empty.
            assertEquals(0, c.localSize());

            Collection<Cache.Entry<Integer, Integer>> res =
                c.query(new SqlQuery<Integer, Integer>(Integer.class, "_key >= 5 order by _key")).getAll();

            assertEquals(5, res.size());

            int i = 5;

            for (Cache.Entry<Integer, Integer> e : res) {
                assertEquals(i, e.getKey().intValue());
                assertEquals(i, e.getValue().intValue());

                i++;
            }
        }
        finally {
            stopGrid("client");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientsLocalQuery() throws Exception {
        try {
            Ignite g = startGrid("client");

            IgniteCache<Integer, Integer> c = jcache(g, Integer.class, Integer.class);

            for (int i = 0; i < 10; i++)
                c.put(i, i);

            assertEquals(0, c.localSize());

            SqlQuery<Integer, Integer> qry = new SqlQuery<>(Integer.class, "_key >= 5 order by _key");

            qry.setLocal(true);

            assertThrowsWithCause(() -> c.query(qry), CacheException.class);
        }
        finally {
            stopGrid("client");
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testIterator() throws Exception {
        int keyCnt = 100;

        for (int i = 0; i < keyCnt; i++)
            cache1.put(new CacheKey(i), new CacheValue("val" + i));

        assertEquals(keyCnt, cache1.localSize(ALL));
        assertEquals(keyCnt, cache2.localSize(ALL));
        assertEquals(keyCnt, cache3.localSize(ALL));

        QueryCursor<Cache.Entry<CacheKey, CacheValue>> qry =
            cache1.query(new SqlQuery<CacheKey, CacheValue>(CacheValue.class, "true"));

        Iterator<Cache.Entry<CacheKey, CacheValue>> iter = qry.iterator();

        assert iter.hasNext();

        int cnt = 0;

        while (iter.hasNext()) {
            iter.next();

            cnt++;
        }

        assertEquals(keyCnt, cnt);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testLocalQueryWithExplicitFlag() throws Exception {
        doTestLocalQuery(true);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testLocalQueryWithoutExplicitFlag() throws Exception {
        doTestLocalQuery(false);
    }

    /**
     * @param loc Explicit query locality flag.
     * @throws Exception if failed.
     */
    private void doTestLocalQuery(boolean loc) throws Exception {
        cache1.clear();

        Transaction tx = ignite1.transactions().txStart();

        try {
            cache1.put(new CacheKey(1), new CacheValue("1"));
            cache1.put(new CacheKey(2), new CacheValue("2"));
            cache1.put(new CacheKey(3), new CacheValue("3"));
            cache1.put(new CacheKey(4), new CacheValue("4"));

            tx.commit();

            info("Committed transaction: " + tx);
        }
        catch (IgniteException e) {
            tx.rollback();

            throw e;
        }

        checkLocalQueryResults(cache1, loc);
        checkLocalQueryResults(cache2, loc);
        checkLocalQueryResults(cache3, loc);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDistributedQuery() throws Exception {
        final int keyCnt = 4;

        Transaction tx = ignite1.transactions().txStart();

        try {
            for (int i = 1; i <= keyCnt; i++)
                cache1.put(new CacheKey(i), new CacheValue(String.valueOf(i)));

            tx.commit();

            info("Committed transaction: " + tx);
        }
        catch (IgniteException e) {
            tx.rollback();

            throw e;
        }

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return cache2.size() == keyCnt && cache3.size() == keyCnt;
            }
        }, 5000);

        QueryCursor<Cache.Entry<CacheKey, CacheValue>> qry =
            cache1.query(new SqlQuery<CacheKey, CacheValue>(CacheValue.class, "val > 1 and val < 4"));

        // Distributed query.
        assertEquals(2, qry.getAll().size());

        // Create new query, old query cannot be modified after it has been executed.
        qry = cache3.query(new SqlQuery<CacheKey, CacheValue>(CacheValue.class, "val > 1 and val < 4").setLocal(true));

        // Tests execute on node.
        Iterator<Cache.Entry<CacheKey, CacheValue>> iter = qry.iterator();

        assert iter != null;
        assert iter.hasNext();

        iter.next();

        assert iter.hasNext();

        iter.next();

        assert !iter.hasNext();
    }

    /**
     * @throws Exception If test failed.
     */
    public void testToString() throws Exception {
        int keyCnt = 4;

        for (int i = 1; i <= keyCnt; i++)
            cache1.put(new CacheKey(i), new CacheValue(String.valueOf(i)));

        // Create query with key filter.

        QueryCursor<Cache.Entry<CacheKey, CacheValue>> qry =
            cache1.query(new SqlQuery<CacheKey, CacheValue>(CacheValue.class, "val > 0"));

        assertEquals(keyCnt, qry.getAll().size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLostIterator() throws Exception {
        IgniteCache<Integer, Integer> cache = jcache(Integer.class, Integer.class);

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        QueryCursor<Cache.Entry<Integer, Integer>> fut = null;

        for (int i = 0; i < cache.getConfiguration(CacheConfiguration.class).getMaxQueryIteratorsCount() + 1; i++) {
            QueryCursor<Cache.Entry<Integer, Integer>> q =
                cache.query(new SqlQuery<Integer, Integer>(Integer.class, "_key >= 0 order by _key"));

            assertEquals(0, (int)q.iterator().next().getKey());

            if (fut == null)
                fut = q;
        }

        final QueryCursor<Cache.Entry<Integer, Integer>> fut0 = fut;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                int i = 0;

                Cache.Entry<Integer, Integer> e;

                while ((e = fut0.iterator().next()) != null)
                    assertEquals(++i, (int)e.getKey());

                return null;
            }
        }, IgniteException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    @IgniteIgnore(value = "https://issues.apache.org/jira/browse/IGNITE-613", forceFailure = true)
    public void testNodeLeft() throws Exception {
        Ignite g = startGrid("client");

        try {
            assertTrue(g.configuration().isClientMode());

            IgniteCache<Integer, Integer> cache = jcache(Integer.class, Integer.class);

            for (int i = 0; i < 1000; i++)
                cache.put(i, i);

            // Client cache should be empty.
            assertEquals(0, cache.localSize());

            QueryCursor<Cache.Entry<Integer, Integer>> q =
                cache.query(new SqlQuery<Integer, Integer>(Integer.class, "_key >= 0 order by _key").setPageSize(10));

            assertEquals(0, (int)q.iterator().next().getKey());

            // Query for replicated cache was run on one of nodes.
            ConcurrentMap<?, ?> mapNode1 = queryResultMap(0);
            ConcurrentMap<?, ?> mapNode2 = queryResultMap(1);
            ConcurrentMap<?, ?> mapNode3 = queryResultMap(2);

            assertEquals(1, mapNode1.size() + mapNode2.size() + mapNode3.size());

            final UUID nodeId = g.cluster().localNode().id();

            final CountDownLatch latch = new CountDownLatch(1);

            grid(0).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    if (((DiscoveryEvent)evt).eventNode().id().equals(nodeId))
                        latch.countDown();

                    return true;
                }
            }, EVT_NODE_LEFT);

            stopGrid("client");

            latch.await();

            assertEquals(0, mapNode1.size());
            assertEquals(0, mapNode2.size());
            assertEquals(0, mapNode3.size());
        }
        finally {
            stopGrid("client");
        }
    }

    /**
     * @param node Node index.
     * @return Query results map.
     */
    private ConcurrentMap<?, ?> queryResultMap(int node) {
        return U.field(((IgniteH2Indexing)U.field(grid(node).context().query(), "idx")).mapQueryExecutor(), "qryRess");
    }

    /**
     * @param cache Cache.
     * @param loc Explicit query locality flag.
     * @throws Exception If check failed.
     */
    private void checkLocalQueryResults(IgniteCache<CacheKey, CacheValue> cache, boolean loc) throws Exception {
        QueryCursor<Cache.Entry<CacheKey, CacheValue>> qry =
            cache.query(new SqlQuery<CacheKey, CacheValue>(CacheValue.class, "val > 1 and val < 4")
                .setReplicatedOnly(true).setLocal(loc));

        Iterator<Cache.Entry<CacheKey, CacheValue>> iter = qry.iterator();

        assert iter != null;
        assert iter.hasNext();

        Cache.Entry<CacheKey, CacheValue> entry = iter.next();

        assert entry.getKey().equals(new CacheKey(2)) || entry.getKey().equals(new CacheKey(3));

        assert iter.hasNext();

        entry = iter.next();

        assert entry.getKey().equals(new CacheKey(2)) || entry.getKey().equals(new CacheKey(3));
        assert !iter.hasNext();
    }

    /**
     * Cache key.
     */
    public static class CacheKey implements Externalizable {
        /** Key. */
        private int key;

        /**
         * @param key Key.
         */
        CacheKey(int key) {
            this.key = key;
        }

        /**
         *
         */
        public CacheKey() {
            /* No-op. */
        }

        /**
         * @return Key.
         */
        public int getKey() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            key = in.readInt();

            keyDesCnt++;

            if (TEST_DEBUG)
                X.println("Deserialized demo key [keyDesCnt=" + keyDesCnt + ", key=" + this + ']');
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(key);

            keySerCnt++;

            if (TEST_DEBUG)
                X.println("Serialized demo key [serCnt=" + keySerCnt + ", key=" + this + ']');
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            CacheKey cacheKey;

            if (o instanceof CacheKey)
                cacheKey = (CacheKey)o;
            else
                return false;

            return key == cacheKey.key;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheKey.class, this);
        }
    }

    /**
     * Cache value..
     */
    public static class CacheValue implements Externalizable {
        /** Value. */
        @QuerySqlField
        private String val;

        /**
         * @param val Value.
         */
        CacheValue(String val) {
            this.val = val;
        }

        /**
         *
         */
        public CacheValue() {
            /* No-op. */
        }

        /**
         * @return Value.
         */
        public String getValue() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            val = U.readString(in);

            valDesCnt++;

            if (TEST_DEBUG)
                X.println("Deserialized demo value [valDesCnt=" + valDesCnt + ", val=" + this + ']');
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, val);

            valSerCnt++;

            if (TEST_DEBUG)
                X.println("Serialized demo value [serCnt=" + valSerCnt + ", val=" + this + ']');
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            CacheValue val = (CacheValue)o;

            return !(this.val != null ? !this.val.equals(val.val) : val.val != null);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val != null ? val.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheValue.class, this);
        }
    }
}
