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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMetricsAdapter;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridStringLogger;

/**
 * Checks behavior on exception while unmarshalling key.
 */
public class IgniteCacheP2pUnmarshallingContinuousQueryErrorTest extends IgniteCacheP2pUnmarshallingErrorTest {
    /** */
    private GridStringLogger stringLogger = new GridStringLogger();

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setGridLogger(stringLogger);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public void testResponseMessageOnUnmarshallingFailed() throws Exception {
        final TestKey testKey = new TestKey(String.valueOf(++key));

        final AtomicInteger unhandledExceptionCounter = new AtomicInteger();

        final CountDownLatch latch = new CountDownLatch(1);

        grid(0).events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event event) {
                unhandledExceptionCounter.incrementAndGet();

                return true;
            }
        }, EventType.EVT_UNHANDLED_EXCEPTION);

        ContinuousQuery<TestKey, String> qry = new ContinuousQuery<>();

        qry.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<TestKey, String>() {
            @Override public boolean apply(TestKey key, String val) {
                return true;
            }
        }));

        qry.setLocalListener(new CacheEntryUpdatedListener<TestKey, String>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends TestKey, ? extends String>> evts) {
                for (CacheEntryEvent<? extends TestKey, ? extends String> e : evts) {
                    assertEquals(e.getKey(), testKey);

                    assertEquals(e.getValue(), "value");

                    latch.countDown();

                    log.debug("key=" + e.getKey() + ", val=" + e.getValue());
                }
            }
        });

        readCnt.set(2); // counter for testKey.readExternal

        try (QueryCursor<Cache.Entry<TestKey, String>> cur = jcache(0).query(qry)) {
            jcache(0).put(testKey, "value");

            assertTrue(!latch.await(1000, TimeUnit.MILLISECONDS));
        }

        GridCacheQueryMetricsAdapter metr = (GridCacheQueryMetricsAdapter)jcache(0).queryMetrics();

        log.debug("QueryMetrics: executions=" + metr.executions() + ", fails=" + metr.fails());

        log.debug("inhandledExceptionCounter=" + unhandledExceptionCounter);

        assertEquals(unhandledExceptionCounter.intValue(), 1);

        assertEquals(metr.executions(), 1);

        assertEquals(metr.fails(), 1);
    }
}
