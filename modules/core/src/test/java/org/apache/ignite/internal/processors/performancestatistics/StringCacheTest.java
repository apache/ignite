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

import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.cacheStartRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.jobRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.taskRecordSize;

/**
 * Tests strings caching.
 */
public class StringCacheTest extends AbstractPerformanceStatisticsTest {
    /** Test value of {@link IgniteSystemProperties#IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD}. */
    private static final int TEST_CACHED_STRINGS_THRESHOLD = 3;

    /** */
    @Test
    public void testCache() {
        StringCache cache = new StringCache();

        assertFalse(cache.cacheIfPossible("A"));

        assertTrue(cache.cacheIfPossible("A"));
        assertTrue(cache.cacheIfPossible("A"));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD, value = "" + TEST_CACHED_STRINGS_THRESHOLD)
    public void testThreshold() {
        StringCache cache = new StringCache();

        assertFalse(cache.cacheIfPossible("1"));
        assertTrue(cache.cacheIfPossible("1"));

        assertFalse(cache.cacheIfPossible("2"));
        assertTrue(cache.cacheIfPossible("2"));

        assertFalse(cache.cacheIfPossible("3"));
        assertFalse(cache.cacheIfPossible("3"));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheTaskName() throws Exception {
        IgniteEx ignite = startGrid(0);

        String testTaskName = "TestTask";
        int executions = 5;

        startCollectStatistics();

        for (int i = 0; i < executions; i++) {
            ignite.compute().withName(testTaskName).run(new IgniteRunnable() {
                @Override public void run() {
                    // No-op.
                }
            });
        }

        AtomicInteger tasks = new AtomicInteger();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void task(UUID nodeId, IgniteUuid sesId, String taskName, long startTime, long duration,
                int affPartId) {
                if (testTaskName.equals(taskName))
                    tasks.incrementAndGet();
            }
        });

        assertEquals(executions, tasks.get());

        long expLen = 1 + OperationType.versionRecordSize();
        expLen += taskRecordSize(testTaskName.getBytes().length, false) +
            taskRecordSize(0, true) * (executions - 1) +
            jobRecordSize() * executions +
            /*opType*/ 2 * executions;

        // Started caches.
        expLen += ignite.context().cache().cacheDescriptors().values().stream().mapToInt(
            desc -> 1 + cacheStartRecordSize(desc.cacheName().getBytes().length, false)).sum();

        List<File> files = statisticsFiles();

        assertEquals(2, files.size());

        File file = performanceStatisticsFiles(files).get(0);

        assertEquals(expLen, file.length());
    }
}
