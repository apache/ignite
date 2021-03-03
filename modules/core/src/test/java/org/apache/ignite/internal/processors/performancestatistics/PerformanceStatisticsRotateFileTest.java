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
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Performance statistics file rotation tests.
 */
public class PerformanceStatisticsRotateFileTest extends AbstractPerformanceStatisticsTest {
    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** Listener test logger. */
    private static ListeningTestLogger listeningLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());
        cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        listeningLog = new ListeningTestLogger(log);

        startGrid(NODES_CNT - 1);

        startClientGrid(NODES_CNT);
    }

    /** @throws Exception If failed. */
    @Test
    public void testRotateFile() throws Exception {
        int cnt = 3;

        assertThrows(log, AbstractPerformanceStatisticsTest::rotateCollectStatistics, IgniteException.class,
            "Performance statistics collection not started.");

        startCollectStatistics();

        AtomicInteger ops = new AtomicInteger();

        TestHandler hnd = new TestHandler() {
            @Override public void cacheOperation(UUID nodeId, OperationType type, int cacheId, long startTime,
                long duration) {
                ops.incrementAndGet();
            }
        };

        for (int i = 0; i < cnt; i++) {
            G.allGrids().forEach(ignite -> ignite.cache(DEFAULT_CACHE_NAME).get(0));

            rotateCollectStatisticsAndAwait();

            List<File> files = statisticsFiles(0 < i ? i : null);

            readFiles(files, hnd);

            assertEquals(NODES_CNT, files.size());

            assertEquals(NODES_CNT, ops.get());

            ops.set(0);
        }

        stopCollectStatistics();

        List<File> files = statisticsFiles();

        readFiles(files, hnd);

        assertEquals(NODES_CNT * (cnt + 1), files.size());
        assertEquals(NODES_CNT * cnt, ops.get());
    }

    /** Rotate collecting performance statistics in the cluster and await. */
    private void rotateCollectStatisticsAndAwait() throws Exception {
        LogListener lsnr = LogListener.matches("Performance statistics writer rotated.")
            .build();

        listeningLog.registerListener(lsnr);

        rotateCollectStatistics();

        assertTrue(waitForCondition(lsnr::check, TIMEOUT));
    }
}
