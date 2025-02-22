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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_FILE_MAX_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_FLUSH_SIZE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.cacheStartRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.jobRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.taskRecordSize;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests public performance statistics properties.
 */
public class PerformanceStatisticsPropertiesTest extends AbstractPerformanceStatisticsTest {
    /** Test value of {@link IgniteSystemProperties#IGNITE_PERF_STAT_FILE_MAX_SIZE}. */
    private static final long TEST_FILE_MAX_SIZE = 1024;

    /** Test value of {@link IgniteSystemProperties#IGNITE_PERF_STAT_FLUSH_SIZE}. */
    private static final int TEST_FLUSH_SIZE = 1024;

    /** Test value of {@link IgniteSystemProperties#IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD}. */
    private static final int TEST_CACHED_STRINGS_THRESHOLD = 5;

    /** Server. */
    private static IgniteEx srv;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srv = startGrid(0);
    }

    /** @throws Exception If failed. */
    @Test
    @WithSystemProperty(key = IGNITE_PERF_STAT_FILE_MAX_SIZE, value = "" + TEST_FILE_MAX_SIZE)
    public void testFileMaxSize() throws Exception {
        long initLen = 1 + OperationType.versionRecordSize();

        initLen += srv.context().cache().cacheDescriptors().values().stream().mapToInt(
            desc -> 1 + cacheStartRecordSize(desc.cacheName().getBytes().length, false)).sum();

        long expOpsCnt = (TEST_FILE_MAX_SIZE - initLen) / (/*typeOp*/1 + OperationType.cacheRecordSize());

        startCollectStatistics();

        for (int i = 0; i < expOpsCnt * 2; i++)
            srv.cache(DEFAULT_CACHE_NAME).get(i);

        AtomicInteger opsCnt = new AtomicInteger();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void cacheOperation(UUID nodeId, OperationType type, int cacheId, long startTime,
                long duration) {
                opsCnt.incrementAndGet();
            }
        });

        assertEquals(expOpsCnt, opsCnt.get());

        long expLen = initLen + opsCnt.get() * (/*typeOp*/1 + OperationType.cacheRecordSize());

        List<File> files = statisticsFiles();

        assertEquals(1, files.size());

        long statFileLen = files.get(0).length();

        assertEquals(expLen, statFileLen);

        assertTrue(statFileLen <= TEST_FILE_MAX_SIZE);
    }

    /** @throws Exception If failed. */
    @Test
    @WithSystemProperty(key = IGNITE_PERF_STAT_FLUSH_SIZE, value = "" + TEST_FLUSH_SIZE)
    public void testFlushSize() throws Exception {
        long initLen = srv.context().cache().cacheDescriptors().values().stream().mapToInt(
            desc -> 1 + cacheStartRecordSize(desc.cacheName().getBytes().length, false)).sum();

        long opsCnt = (TEST_FLUSH_SIZE - initLen) / (/*typeOp*/1 + OperationType.cacheRecordSize());

        startCollectStatistics();

        for (int i = 0; i < opsCnt; i++)
            srv.cache(DEFAULT_CACHE_NAME).get(i);

        List<File> files = statisticsFiles();

        assertEquals(1, files.size());
        assertEquals(0, files.get(0).length());

        srv.cache(DEFAULT_CACHE_NAME).get(0);

        assertTrue(waitForCondition(
            () -> {
                try {
                    List<File> statFiles = statisticsFiles();

                    assertEquals(1, statFiles.size());

                    return statFiles.get(0).length() > 0;
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            },
            getTestTimeout()));

        stopCollectStatisticsAndRead(new TestHandler());
    }

    /** @throws Exception If failed. */
    @Test
    @WithSystemProperty(key = IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD, value = "" + TEST_CACHED_STRINGS_THRESHOLD)
    public void testCachedStringsThreshold() throws Exception {
        int tasksCnt = TEST_CACHED_STRINGS_THRESHOLD * 2;
        int executions = 2;

        startCollectStatistics();

        int expLen = 1 + OperationType.versionRecordSize();

        for (int i = 0; i < tasksCnt; i++) {
            String taskName = "TestTask-" + i;

            if (i < TEST_CACHED_STRINGS_THRESHOLD - 1 - srv.context().cache().cacheDescriptors().values().size()) {
                expLen += taskRecordSize(taskName.getBytes().length, false) + jobRecordSize() +
                    (taskRecordSize(0, true) + jobRecordSize()) * (executions - 1);
            }
            else
                expLen += (taskRecordSize(taskName.getBytes().length, false) + jobRecordSize()) * executions;

            expLen += /*typeOp*/2 * executions;

            for (int j = 0; j < executions; j++) {
                srv.compute().withName(taskName).run(new IgniteRunnable() {
                    @Override public void run() {
                        // No-op.
                    }
                });
            }
        }

        AtomicInteger tasks = new AtomicInteger();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void task(UUID nodeId, IgniteUuid sesId, String taskName, long startTime, long duration,
                int affPartId) {
                tasks.incrementAndGet();
            }
        });

        assertEquals(tasksCnt * executions, tasks.get());

        // Started caches.
        expLen += srv.context().cache().cacheDescriptors().values().stream().mapToInt(
            desc -> 1 + cacheStartRecordSize(desc.cacheName().getBytes().length, false)).sum();

        List<File> files = statisticsFiles();

        assertEquals(1, files.size());
        assertEquals(expLen, files.get(0).length());
    }
}
