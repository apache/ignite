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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.UnhandledExceptionEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.ThreadPoolMXBeanAdapter;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.IgniteCacheP2pUnmarshallingErrorTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMetricsAdapter;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.communication.GridCacheMessageSelfTest;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Checks behavior on exception while unmarshalling key.
 */
public class IgniteCacheP2pUnmarshallingContinuousQueryErrorTest extends IgniteCacheP2pUnmarshallingErrorTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** Used inside InitialQuery listener */
    private static CountDownLatch latchInitialQuery = new CountDownLatch(1);

    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cacheCfg =  super.cacheConfiguration(gridName);

        cacheCfg.setStatisticsEnabled(true);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override public void testResponseMessageOnUnmarshallingFailed() throws Exception {
        final AtomicInteger unhandledExceptionCounter = new AtomicInteger();

        grid(0).events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event event) {
                UnhandledExceptionEvent uex = (UnhandledExceptionEvent)event;

                String exceptionMsg = X.getFullStackTrace(uex.getException());

                assertTrue(exceptionMsg.contains("IOException: Class can not be unmarshalled"));

                unhandledExceptionCounter.incrementAndGet();

                return true;
            }
        }, EventType.EVT_UNHANDLED_EXCEPTION);

        grid(1).events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event event) {
                fail("This line newer calls");
                return true;
            }
        }, EventType.EVT_UNHANDLED_EXCEPTION);

        ContinuousQuery<TestKey, String> qry = new ContinuousQuery<>();

        qry.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<TestKey, String>() {
            @Override public boolean apply(TestKey key, String val) {
                latchInitialQuery.countDown();
                return true;
            }
        }));

        qry.setLocalListener(new CacheEntryUpdatedListener<TestKey, String>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends TestKey, ? extends String>> evts) {
                fail("This line newer calls");
            }
        });

        // Before test
        validateCacheQueryMetrics(jcache(0), 0, 0, 0);
        validateCacheQueryMetrics(jcache(1), 0, 0, 0);
        validateCacheQueryMetrics(jcache(2), 0, 0, 0);

        assertEquals(unhandledExceptionCounter.intValue(), 0);

        readCnt.set(100);

        // Put element before creating QueryCursor.
        jcache(1).put(generateNodeKeys(grid(1), jcache(1), "key"), "Hello primary node");

        try (QueryCursor<Cache.Entry<TestKey, String>> cur = jcache(0).query(qry)) {
            latchInitialQuery.await();

            jcache(1).clear();

            readCnt.set(3);

            addEntityAndValidate(1, 1);

            assertEquals(unhandledExceptionCounter.intValue(), 1);

            readCnt.set(3);

            addEntityAndValidate(2, 2);

            assertEquals(unhandledExceptionCounter.intValue(), 2);
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void addEntityAndValidate(int nodeIndex, final int failsNum) throws Exception {
        TestKey keyPrimary = generateNodeKeys(grid(1), jcache(1), "key" + nodeIndex);

        jcache(nodeIndex).put(keyPrimary, "Hello node");

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                GridCacheQueryMetricsAdapter metr = (GridCacheQueryMetricsAdapter)jcache(0).queryMetrics();

                return metr.completedExecutions() == 1 && metr.fails() == failsNum;
            }
        }, 3000));

        validateCacheQueryMetrics(jcache(0), 1, 1, failsNum);
        validateCacheQueryMetrics(jcache(1), 0, 0, 0);
        validateCacheQueryMetrics(jcache(2), 0, 0, 0);
    }

    /**
     * @throws Exception If failed.
     */
    private void validateCacheQueryMetrics(IgniteCache cache, int executions, int completed, int faild) {
        GridCacheQueryMetricsAdapter metr = (GridCacheQueryMetricsAdapter)cache.queryMetrics();

        System.out.println(metr);

        assertEquals(metr.executions(), executions);

        assertEquals(metr.completedExecutions(), completed);

        assertEquals(metr.fails(), faild);
    }


    /**
     * @param node Node.
     * @param cache Cache.
     * @param prefix Prefix.
     */
    private TestKey generateNodeKeys(IgniteEx node, IgniteCache cache, String prefix) {

        ClusterNode locNode = node.localNode();

        Affinity<TestKey> aff = (Affinity<TestKey>)affinity(cache);

        for (int ind = 0; ind < 100_000; ind++) {
            TestKey key = new TestKey(prefix + ind);

            if (aff.isPrimary(locNode, key))
                return key;
        }

        throw new IgniteException("Unable to find " + prefix + " keys as primary for cache.");
    }
}
