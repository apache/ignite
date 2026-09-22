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

package org.apache.ignite.internal.metric;

import java.util.List;
import java.util.function.Consumer;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.spi.systemview.view.ContinuousQueryView;
import org.apache.ignite.spi.systemview.view.ScanQueryView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.junit.Test;

import static org.apache.ignite.internal.managers.systemview.ScanQuerySystemView.SCAN_QRY_SYS_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheGroupId;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.CQ_SYS_VIEW;
import static org.apache.ignite.internal.util.IgniteUtils.toStringSafe;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Tests for {@link SystemView} for queries. */
public class SystemViewQueriesTest extends SystemViewAbstractTest {
    /** */
    public static final String TEST_PREDICATE = "TestPredicate";

    /** */
    public static final String TEST_TRANSFORMER = "TestTransformer";

    /** Tests work of {@link SystemView} for continuous queries. */
    @Test
    public void testContinuousQuery() throws Exception {
        try (IgniteEx originNode = startGrid(0); IgniteEx remoteNode = startGrid(1)) {
            IgniteCache<Integer, Integer> cache = originNode.createCache("cache-1");

            SystemView<ContinuousQueryView> origQrys = originNode.context().systemView().view(CQ_SYS_VIEW);
            SystemView<ContinuousQueryView> remoteQrys = remoteNode.context().systemView().view(CQ_SYS_VIEW);

            assertEquals(0, origQrys.size());
            assertEquals(0, remoteQrys.size());

            try (QueryCursor qry = cache.query(new ContinuousQuery<>()
                .setInitialQuery(new ScanQuery<>())
                .setPageSize(100)
                .setTimeInterval(1000)
                .setLocalListener(evts -> {
                    // No-op.
                })
                .setRemoteFilterFactory(() -> evt -> true)
            )) {
                for (int i = 0; i < 100; i++)
                    cache.put(i, i);

                checkContinuousQueryView(originNode, origQrys, true);
                checkContinuousQueryView(originNode, remoteQrys, false);
            }

            assertEquals(0, origQrys.size());
            assertTrue(waitForCondition(() -> remoteQrys.size() == 0, getTestTimeout()));
        }
    }

    /** */
    private void checkContinuousQueryView(IgniteEx g, SystemView<ContinuousQueryView> qrys, boolean loc) {
        assertEquals(1, qrys.size());

        for (ContinuousQueryView cq : qrys) {
            assertEquals("cache-1", cq.cacheName());
            assertEquals(100, cq.bufferSize());
            assertEquals(1000, cq.interval());
            assertEquals(g.localNode().id(), cq.nodeId());

            if (loc)
                assertTrue(cq.localListener().startsWith(getClass().getName()));
            else
                assertNull(cq.localListener());

            assertTrue(cq.remoteFilter().startsWith(getClass().getName()));
            assertNull(cq.localTransformedListener());
            assertNull(cq.remoteTransformer());
        }
    }

    /** Tests work of {@link SystemView} for local scan queries. */
    @Test
    public void testLocalScanQuery() throws Exception {
        try (IgniteEx g0 = startGrid(0)) {
            IgniteCache<Integer, Integer> cache1 = g0.createCache(
                new CacheConfiguration<Integer, Integer>("cache1")
                    .setGroupName("group1"));

            int part = g0.affinity("cache1").primaryPartitions(g0.localNode())[0];

            List<Integer> partKeys = partitionKeys(cache1, part, 11, 0);

            for (Integer key : partKeys)
                cache1.put(key, key);

            SystemView<ScanQueryView> qrySysView0 = g0.context().systemView().view(SCAN_QRY_SYS_VIEW);

            assertNotNull(qrySysView0);

            assertEquals(0, qrySysView0.size());

            QueryCursor<Integer> qryRes1 = cache1.query(
                new ScanQuery<Integer, Integer>()
                    .setFilter(new TestPredicate())
                    .setLocal(true)
                    .setPartition(part)
                    .setPageSize(10),
                new TestTransformer());

            assertTrue(qryRes1.iterator().hasNext());

            boolean res = waitForCondition(() -> qrySysView0.size() > 0, 5_000);

            assertTrue(res);

            ScanQueryView view = qrySysView0.iterator().next();

            assertEquals(g0.localNode().id(), view.originNodeId());
            assertEquals(0, view.queryId());
            assertEquals("cache1", view.cacheName());
            assertEquals(cacheId("cache1"), view.cacheId());
            assertEquals(cacheGroupId("cache1", "group1"), view.cacheGroupId());
            assertEquals("group1", view.cacheGroupName());
            assertTrue(view.startTime() <= System.currentTimeMillis());
            assertTrue(view.duration() >= 0);
            assertFalse(view.canceled());
            assertEquals(TEST_PREDICATE, view.filter());
            assertTrue(view.local());
            assertEquals(part, view.partition());
            assertEquals(toStringSafe(g0.context().discovery().topologyVersionEx()), view.topology());
            assertEquals(TEST_TRANSFORMER, view.transformer());
            assertFalse(view.keepBinary());
            assertNull(view.subjectId());
            assertNull(view.taskName());

            qryRes1.close();

            res = waitForCondition(() -> qrySysView0.size() == 0, 5_000);

            assertTrue(res);
        }
    }

    /** Tests work of {@link SystemView} for scan queries. */
    @Test
    public void testScanQuery() throws Exception {
        try (IgniteEx g0 = startGrid(0);
            IgniteEx g1 = startGrid(1);
            IgniteEx client1 = startClientGrid("client-1");
            IgniteEx client2 = startClientGrid("client-2")) {

            IgniteCache<Integer, Integer> cache1 = client1.createCache(
                new CacheConfiguration<Integer, Integer>("cache1")
                    .setGroupName("group1"));

            IgniteCache<Integer, Integer> cache2 = client2.createCache("cache2");

            for (int i = 0; i < 100; i++) {
                cache1.put(i, i);
                cache2.put(i, i);
            }

            SystemView<ScanQueryView> qrySysView0 = g0.context().systemView().view(SCAN_QRY_SYS_VIEW);
            SystemView<ScanQueryView> qrySysView1 = g1.context().systemView().view(SCAN_QRY_SYS_VIEW);

            assertNotNull(qrySysView0);
            assertNotNull(qrySysView1);

            assertEquals(0, qrySysView0.size());
            assertEquals(0, qrySysView1.size());

            QueryCursor<Integer> qryRes1 = cache1.query(
                new ScanQuery<Integer, Integer>()
                    .setFilter(new TestPredicate())
                    .setPageSize(10),
                new TestTransformer());

            QueryCursor<?> qryRes2 = cache2.withKeepBinary().query(new ScanQuery<>()
                .setPageSize(20));

            assertTrue(qryRes1.iterator().hasNext());
            assertTrue(qryRes2.iterator().hasNext());

            checkScanQueryView(client1, client2, qrySysView0);
            checkScanQueryView(client1, client2, qrySysView1);

            qryRes1.close();
            qryRes2.close();

            boolean res = waitForCondition(
                () -> qrySysView0.size() + qrySysView1.size() == 0, 5_000);

            assertTrue(res);
        }
    }

    /** */
    private void checkScanQueryView(IgniteEx client1, IgniteEx client2, SystemView<ScanQueryView> qrySysView)
        throws Exception {
        boolean res = waitForCondition(() -> qrySysView.size() > 1, 5_000);

        assertTrue(res);

        Consumer<ScanQueryView> cache1checker = view -> {
            assertEquals(client1.localNode().id(), view.originNodeId());
            assertTrue(view.queryId() != 0);
            assertEquals("cache1", view.cacheName());
            assertEquals(cacheId("cache1"), view.cacheId());
            assertEquals(cacheGroupId("cache1", "group1"), view.cacheGroupId());
            assertEquals("group1", view.cacheGroupName());
            assertTrue(view.startTime() <= System.currentTimeMillis());
            assertTrue(view.duration() >= 0);
            assertFalse(view.canceled());
            assertEquals(TEST_PREDICATE, view.filter());
            assertFalse(view.local());
            assertEquals(-1, view.partition());
            assertEquals(toStringSafe(client1.context().discovery().topologyVersionEx()), view.topology());
            assertEquals(TEST_TRANSFORMER, view.transformer());
            assertFalse(view.keepBinary());
            assertNull(view.subjectId());
            assertNull(view.taskName());
            assertEquals(10, view.pageSize());
        };

        Consumer<ScanQueryView> cache2checker = view -> {
            assertEquals(client2.localNode().id(), view.originNodeId());
            assertTrue(view.queryId() != 0);
            assertEquals("cache2", view.cacheName());
            assertEquals(cacheId("cache2"), view.cacheId());
            assertEquals(cacheGroupId("cache2", null), view.cacheGroupId());
            assertEquals("cache2", view.cacheGroupName());
            assertTrue(view.startTime() <= System.currentTimeMillis());
            assertTrue(view.duration() >= 0);
            assertFalse(view.canceled());
            assertNull(view.filter());
            assertFalse(view.local());
            assertEquals(-1, view.partition());
            assertEquals(toStringSafe(client2.context().discovery().topologyVersionEx()), view.topology());
            assertNull(view.transformer());
            assertTrue(view.keepBinary());
            assertNull(view.subjectId());
            assertNull(view.taskName());
            assertEquals(20, view.pageSize());
        };

        boolean found1 = false;
        boolean found2 = false;

        for (ScanQueryView view : qrySysView) {
            if ("cache2".equals(view.cacheName())) {
                cache2checker.accept(view);
                found1 = true;
            }
            else {
                cache1checker.accept(view);
                found2 = true;
            }
        }

        assertTrue(found1 && found2);
    }

    /** Test predicate. */
    public static class TestPredicate implements IgniteBiPredicate<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public boolean apply(Integer integer, Integer integer2) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return TEST_PREDICATE;
        }
    }

    /** Test transformer. */
    public static class TestTransformer implements IgniteClosure<javax.cache.Cache.Entry<Integer, Integer>, Integer> {
        /** {@inheritDoc} */
        @Override public Integer apply(Cache.Entry<Integer, Integer> entry) {
            return entry.getKey();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return TEST_TRANSFORMER;
        }
    }
}
