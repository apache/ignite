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

import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.UnhandledExceptionEvent;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMetricsAdapter;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Checks behavior on exception while unmarshalling key.
 */
public class IgniteCacheP2pUnmarshallingContinuousQueryErrorTest extends IgniteCacheP2pUnmarshallingErrorTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
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

        ContinuousQuery<TestKey, String> qry = new ContinuousQuery<>();

        qry.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<TestKey, String>() {
            @Override public boolean apply(TestKey key, String val) {
                return true;
            }
        }));

        qry.setLocalListener(new CacheEntryUpdatedListener<TestKey, String>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends TestKey, ? extends String>> evts) {
                fail("This line newer calls");
            }
        });

        readCnt.set(2); // counter for testKey.readExternal

        GridCacheQueryMetricsAdapter metr = (GridCacheQueryMetricsAdapter)jcache(0).queryMetrics();

        assertEquals(metr.fails(), 0);
        assertEquals(metr.executions(), 0);
        assertEquals(unhandledExceptionCounter.intValue(), 0);

        try (QueryCursor<Cache.Entry<TestKey, String>> cur = jcache(0).query(qry)) {

            TestKey testKey = new TestKey("key1");

            // this line run exception on server
            jcache(1).put(testKey, "value1");

            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    GridCacheQueryMetricsAdapter metr = (GridCacheQueryMetricsAdapter)jcache(0).queryMetrics();
                    return metr.fails() == 1 && metr.executions() == 1 && unhandledExceptionCounter.intValue() == 1;
                }
            }, 5000));
        }
    }
}