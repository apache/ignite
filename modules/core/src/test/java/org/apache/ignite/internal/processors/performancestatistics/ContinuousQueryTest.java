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

package org.apache.ignite.internal.processors.performancestatistics;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryManager;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CQ_ENTRY_FILTERED;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CQ_ENTRY_PROCESSED;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CQ_ENTRY_TRANSFORMED;

/**
 * Tests performance statistics.
 */
public class ContinuousQueryTest extends AbstractPerformanceStatisticsTest {
    /** Ignite. */
    private static IgniteEx srv;

    /** Test cache. */
    private static IgniteCache<Object, Object> cache;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        srv = startGrid(0);

        cache = srv.cache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cache.clear();
    }

    /** @throws Exception If failed. */
    @Test
    public void testInternalQuery() throws Exception {
        int expFilterDur = 1000;

        cache.put(1, 1);

        CacheContinuousQueryManager contQryMgr = srv.context().cache().context().cacheContext(CU.cacheId(DEFAULT_CACHE_NAME))
            .continuousQueries();

        CountDownLatch evtLatch = new CountDownLatch(1);

        startCollectStatistics();

        long totalDur;

        long startTimeNanos = System.nanoTime();

        contQryMgr.executeInternalQuery(
            evts -> evts.forEach(e -> evtLatch.countDown()),
            e -> {
                try {
                    U.sleep(expFilterDur);
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    // No-op.
                }

                return true;
            }, true, true, false, false);

        evtLatch.await();

        totalDur = System.nanoTime() - startTimeNanos;

        Set<OperationType> expOps = new HashSet<>(Arrays.asList(CQ_ENTRY_FILTERED, CQ_ENTRY_PROCESSED));

        AtomicLong filterDur = new AtomicLong();
        AtomicLong processDur = new AtomicLong();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void continuousQueryOperation(UUID nodeId, OperationType type, UUID routineId,
                long opStartTime, long duration, int entCnt) {
                assertTrue(expOps.remove(type));

                if (type == CQ_ENTRY_FILTERED)
                    filterDur.set(duration);

                if (type == CQ_ENTRY_PROCESSED)
                    processDur.set(duration);
            }
        });

        assertTrue(expOps.isEmpty());

        assertTrue(processDur.get() + filterDur.get() <= totalDur);
    }

    /** @throws Exception If failed. */
    @Test
    public void testLocalQueryWithTransformer() throws Exception {
        int expTransformDur = 1000;

        CountDownLatch evtLatch = new CountDownLatch(1);

        ContinuousQueryWithTransformer<Integer, Integer, Integer> qry = new ContinuousQueryWithTransformer<>();

        qry.setLocal(true);

        qry.setLocalListener(evts -> evts.forEach(e -> evtLatch.countDown()));

        qry.setRemoteTransformerFactory(() -> e -> {
            try {
                U.sleep(expTransformDur);
            }
            catch (IgniteInterruptedCheckedException ignored) {
                // No-op.
            }

            return e.getValue();
        });

        startCollectStatistics();

        long totalDur;

        try (QueryCursor<Cache.Entry<Integer, Integer>> cur = cache.query(qry)) {
            long startTimeNanos = System.nanoTime();

            cache.put(1, 1);

            evtLatch.await();

            totalDur = System.nanoTime() - startTimeNanos;
        }

        Set<OperationType> expOps = new HashSet<>(Arrays.asList(CQ_ENTRY_PROCESSED, CQ_ENTRY_TRANSFORMED));

        AtomicLong transformDur = new AtomicLong();
        AtomicLong processDur = new AtomicLong();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void continuousQueryOperation(UUID nodeId, OperationType type, UUID routineId,
                long opStartTime, long duration, int entCnt) {
                assertTrue(expOps.remove(type));

                if (type == CQ_ENTRY_PROCESSED)
                    processDur.set(duration);

                if (type == CQ_ENTRY_TRANSFORMED)
                    transformDur.set(duration);
            }
        });

        assertTrue(expOps.isEmpty());

        assertTrue(processDur.get() + transformDur.get() <= totalDur);
    }
}