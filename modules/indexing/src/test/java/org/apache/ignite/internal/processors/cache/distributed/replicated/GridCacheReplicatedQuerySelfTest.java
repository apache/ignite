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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;
import org.springframework.util.*;

import java.io.*;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.events.EventType.*;

/**
 * Tests replicated query.
 */
public class GridCacheReplicatedQuerySelfTest extends GridCacheAbstractQuerySelfTest {
    /** */
    private static final boolean TEST_DEBUG = false;

    /** Grid1. */
    private static Ignite ignite1;

    /** Grid2. */
    private static Ignite ignite2;

    /** Grid3. */
    private static Ignite ignite3;

    /** Cache1. */
    private static GridCache<CacheKey, CacheValue> cache1;

    /** Cache2. */
    private static GridCache<CacheKey, CacheValue> cache2;

    /** Cache3. */
    private static GridCache<CacheKey, CacheValue> cache3;

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
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ignite1 = grid(0);
        ignite2 = grid(1);
        ignite3 = grid(2);

        cache1 = ignite1.cache(null);
        cache2 = ignite2.cache(null);
        cache3 = ignite3.cache(null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientOnlyNode() throws Exception {
        try {
            Ignite g = startGrid("client");

            GridCache<Integer, Integer> c = ((IgniteKernal)g).cache(null);

            for (int i = 0; i < 10; i++)
                c.putx(i, i);

            // Client cache should be empty.
            assertEquals(0, c.size());

            Collection<Map.Entry<Integer, Integer>> res =
                c.queries().createSqlQuery(Integer.class, "_key >= 5 order by _key").execute().get();

            assertEquals(5, res.size());

            Iterator<Map.Entry<Integer, Integer>> it = res.iterator();

            int i = 5;

            while (it.hasNext()) {
                Map.Entry<Integer, Integer> e  = it.next();

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
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testIterator() throws Exception {
        int keyCnt = 100;

        for (int i = 0; i < keyCnt; i++)
            cache1.put(new CacheKey(i), new CacheValue("val" + i));

        assertEquals(keyCnt, cache1.size());
        assertEquals(keyCnt, cache2.size());
        assertEquals(keyCnt, cache3.size());

        CacheQuery<Map.Entry<CacheKey, CacheValue>> qry = cache1.queries().createSqlQuery(CacheValue.class,
            "select * from CacheValue").projection(ignite.cluster());

        qry.pageSize(10);
        qry.enableDedup(false);

        Iterator<Map.Entry<CacheKey, CacheValue>> iter = qry.execute().get().iterator();

        assert iter.hasNext();

        int cnt = 0;

        while (iter.hasNext()) {
            iter.next();

            cnt++;
        }

        // Expect duplicates since we run query on full projection of 3 nodes and dedup flag is false.
        assertEquals(keyCnt * 3, cnt);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testLocalQuery() throws Exception {
        cache1.clear();

        IgniteTx tx = cache1.txStart();

        try {
            cache1.put(new CacheKey(1), new CacheValue("1"));
            cache1.put(new CacheKey(2), new CacheValue("2"));
            cache1.put(new CacheKey(3), new CacheValue("3"));
            cache1.put(new CacheKey(4), new CacheValue("4"));

            tx.commit();

            info("Committed transaction: " + tx);
        }
        catch (IgniteCheckedException e) {
            tx.rollback();

            throw e;
        }

        checkQueryResults(ignite1, cache1);
        checkQueryResults(ignite2, cache2);
        checkQueryResults(ignite3, cache3);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDistributedQuery() throws Exception {
        int keyCnt = 4;

        final CountDownLatch latch = new CountDownLatch(keyCnt * 2);

        IgnitePredicate<Event> lsnr = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                latch.countDown();

                return true;
            }
        };

        ignite2.events().localListen(lsnr, EventType.EVT_CACHE_OBJECT_PUT);
        ignite3.events().localListen(lsnr, EventType.EVT_CACHE_OBJECT_PUT);

        IgniteTx tx = cache1.txStart();

        try {
            for (int i = 1; i <= keyCnt; i++)
                cache1.put(new CacheKey(i), new CacheValue(String.valueOf(i)));

            tx.commit();

            info("Committed transaction: " + tx);
        }
        catch (IgniteCheckedException e) {
            tx.rollback();

            throw e;
        }

        latch.await();

        CacheQuery<Map.Entry<CacheKey, CacheValue>> qry = cache1.queries().createSqlQuery(
            CacheValue.class, "val > 1 and val < 4").projection(ignite.cluster());

        qry.enableDedup(false);

        qry.keepAll(true);

        // Distributed query.
        assertEquals(6, qry.execute().get().size());

        // Create new query, old query cannot be modified after it has been executed.
        qry = cache1.queries().createSqlQuery(CacheValue.class, "val > 1 and val < 4");

        qry = qry.projection(ignite3.cluster().forLocal());

        // Tests execute on node.
        Iterator<Map.Entry<CacheKey, CacheValue>> iter = qry.execute().get().iterator();

        assert iter != null;
        assert iter.hasNext();

        iter.next();

        assert iter.hasNext();

        iter.next();

        assert !iter.hasNext();
    }

    /**
     * Returns private field {@code qryIters} of {@link GridCacheQueryManager} for the given grid.
     *
     * @param g Grid which {@link GridCacheQueryManager} should be observed.
     * @return {@code qryIters} of {@link GridCacheQueryManager}.
     */
    private ConcurrentMap<UUID,
        Map<Long, GridFutureAdapter<GridCloseableIterator<IgniteBiTuple<CacheKey, CacheValue>>>>>
        distributedQueryManagerQueryItersMap(Ignite g) {
        GridCacheContext ctx = ((IgniteKernal)g).internalCache().context();

        Field qryItersField = ReflectionUtils.findField(ctx.queries().getClass(), "qryIters");

        qryItersField.setAccessible(true);

        return (ConcurrentMap<UUID,
            Map<Long, GridFutureAdapter<GridCloseableIterator<IgniteBiTuple<CacheKey, CacheValue>>>>>)
            ReflectionUtils.getField(qryItersField, ctx.queries());
    }

    /**
     * @throws Exception If test failed.
     */
    @Override public void testReduceQuery() throws Exception {
        int keyCnt = 4;

        for (int i = 1; i <= keyCnt; i++)
            cache1.put(new CacheKey(i), new CacheValue(String.valueOf(i)));

        CacheQuery<Map.Entry<CacheKey, CacheValue>> rdcQry =
            cache1.queries().createSqlQuery(CacheValue.class, "val > 1 and val < 4");

        rdcQry.keepAll(true);

        Iterator<Integer> reduceIter = rdcQry.execute(new IgniteReducer<Map.Entry<CacheKey, CacheValue>, Integer>() {
            /** Reducer result. */
            private final AtomicInteger res = new AtomicInteger();

            @Override public boolean collect(Map.Entry<CacheKey, CacheValue> e) {
                res.addAndGet(e.getKey().getKey());

                return true;
            }

            @Override public Integer reduce() {
                return res.addAndGet(5);
            }
        }).get().iterator();

        assert reduceIter != null;
        assert reduceIter.hasNext();
        assert reduceIter.next() == 10;
        assert !reduceIter.hasNext();

        // Tests reduce query where remote reducer returns null.
        CacheQuery<Map.Entry<CacheKey, CacheValue>> rdcQry1 =
            cache1.queries().createSqlQuery(CacheValue.class, "val > 1 and val < 4");

        rdcQry1.keepAll(true);

        Iterator<Object> reduceIter1 = rdcQry1.execute(new IgniteReducer<Map.Entry<CacheKey, CacheValue>, Object>() {
            @Override public boolean collect(Map.Entry<CacheKey, CacheValue> e) {
                // No-op.
                return true;
            }

            @Nullable @Override public Object reduce() {
                return null;
            }
        }).get().iterator();

        assert reduceIter1 != null;
        assert reduceIter1.hasNext();
        assertNull(reduceIter1.next());
        assert !reduceIter.hasNext();
    }

    /**
     * @throws Exception If test failed.
     */
    public void testToString() throws Exception {
        int keyCnt = 4;

        for (int i = 1; i <= keyCnt; i++)
            cache1.put(new CacheKey(i), new CacheValue(String.valueOf(i)));

        // Create query with key filter.
        CacheQuery<Map.Entry<CacheKey, CacheValue>> qry =
            cache1.queries().createSqlQuery(CacheValue.class, "val > 0").projection(ignite.cluster());

        qry.keepAll(true);

        CacheQueryFuture<Map.Entry<CacheKey, CacheValue>> fut = qry.execute();

        assertEquals(keyCnt * 3, fut.get().size());

        info("Query result: " + fut.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLostIterator() throws Exception {
        GridCache<Integer, Integer> cache = ((IgniteKernal)ignite).cache(null);

        for (int i = 0; i < 1000; i++)
            assertTrue(cache.putx(i, i));

        CacheQueryFuture<Map.Entry<Integer, Integer>> fut = null;

        for (int i = 0; i < cache.configuration().getMaximumQueryIteratorCount() + 1; i++) {
            CacheQuery<Map.Entry<Integer, Integer>> q = cache.queries().createSqlQuery(Integer.class,
                "_key >= 0 order by _key").projection(ignite.cluster());

            q.pageSize(200);
            q.enableDedup(true);

            CacheQueryFuture<Map.Entry<Integer, Integer>> f = q.execute();

            assertEquals(0, (int)f.next().getKey());

            if (fut == null)
                fut = f;
        }

        final CacheQueryFuture<Map.Entry<Integer, Integer>> fut0 = fut;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                int i = 0;

                Map.Entry<Integer, Integer> e;

                while ((e = fut0.next()) != null)
                    assertEquals(++i, (int)e.getKey());

                return null;
            }
        }, IgniteException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeLeft() throws Exception {
        try {
            Ignite g = startGrid();

            GridCache<Integer, Integer> cache = ((IgniteKernal)g).cache(null);

            for (int i = 0; i < 1000; i++)
                assertTrue(cache.putx(i, i));

            CacheQuery<Map.Entry<Integer, Integer>> q = cache.queries().createSqlQuery(Integer.class,
                "_key >= 0 order by _key").projection(ignite.cluster());

            q.pageSize(50);

            q.projection(g.cluster().forNodes(Arrays.asList(g.cluster().localNode(), grid(0).localNode())));

            CacheQueryFuture<Map.Entry<Integer, Integer>> fut = q.execute();

            assertEquals(0, (int)fut.next().getKey());

            final ConcurrentMap<UUID, Map<Long, GridFutureAdapter<GridCloseableIterator<
                IgniteBiTuple<Integer, Integer>>>>> map =
                U.field(((IgniteKernal)grid(0)).internalCache().context().queries(), "qryIters");

            // fut.nextX() does not guarantee the request has completed on remote node
            // (we could receive page from local one), so we need to wait.
            assertTrue(GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    return map.size() == 1;
                }
            }, getTestTimeout()));

            Map<Long, GridFutureAdapter<GridCloseableIterator<IgniteBiTuple<Integer, Integer>>>> futs =
                map.get(g.cluster().localNode().id());

            assertEquals(1, futs.size());

            GridCloseableIterator<IgniteBiTuple<Integer, Integer>> iter =
                (GridCloseableIterator<IgniteBiTuple<Integer, Integer>>)((IgniteInternalFuture)F.first(futs.values()).get()).get();

            ResultSet rs = U.field(iter, "data");

            assertFalse(rs.isClosed());

            final UUID nodeId = g.cluster().localNode().id();
            final CountDownLatch latch = new CountDownLatch(1);

            grid(0).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    if (((DiscoveryEvent)evt).eventNode().id().equals(nodeId))
                        latch.countDown();

                    return true;
                }
            }, EVT_NODE_LEFT);

            stopGrid();

            latch.await();

            assertEquals(0, map.size());
            assertTrue(rs.isClosed());
        }
        finally {
            // Ensure that additional node is stopped.
            stopGrid();
        }
    }

    /**
     * @param ignite Grid.
     * @param cache Cache.
     * @throws Exception If check failed.
     */
    private void checkQueryResults(Ignite ignite, CacheProjection<CacheKey, CacheValue> cache) throws Exception {
        CacheQuery<Map.Entry<CacheKey, CacheValue>> qry =
            cache.queries().createSqlQuery(CacheValue.class, "val > 1 and val < 4");

        CacheQuery<Map.Entry<CacheKey, CacheValue>> q = qry.projection(ignite.cluster().forLocal());

        Iterator<Map.Entry<CacheKey, CacheValue>> iter = q.execute().get().iterator();

        assert iter != null;
        assert iter.hasNext();

        Map.Entry<CacheKey, CacheValue> entry = iter.next();

        assert entry.getKey().equals(new CacheKey(2)) || entry.getKey().equals(new CacheKey(3));

        assert iter.hasNext();

        entry = iter.next();

        assert entry.getKey().equals(new CacheKey(2)) || entry.getKey().equals(new CacheKey(3));
        assert !iter.hasNext();

        for (ClusterNode node : ignite.cluster().forRemotes().nodes()) {
            q = qry.projection(ignite1.cluster().forNode(node));

            iter = q.execute().get().iterator();

            assert iter != null;
            assert iter.hasNext();

            entry = iter.next();

            assert entry.getKey().equals(new CacheKey(2)) || entry.getKey().equals(new CacheKey(3));

            assert iter.hasNext();

            entry = iter.next();

            assert entry.getKey().equals(new CacheKey(2)) || entry.getKey().equals(new CacheKey(3));
            assert !iter.hasNext();
        }
    }

    /**
     * Cache key.
     */
    private static class CacheKey implements Externalizable {
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
    private static class CacheValue implements Externalizable {
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
