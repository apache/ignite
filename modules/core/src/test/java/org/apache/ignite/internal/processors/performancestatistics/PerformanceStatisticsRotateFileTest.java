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
import java.util.ArrayList;
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
import static org.apache.ignite.testframework.LogListener.matches;

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

        startGrids(NODES_CNT - 1);

        startClientGrid(NODES_CNT);
    }

    /** @throws Exception If failed. */
    @Test
    public void testRotateFile() throws Exception {
        assertThrows(log, AbstractPerformanceStatisticsTest::rotateCollectStatistics, IgniteException.class,
            "Performance statistics collection not started.");

        startCollectStatistics();

        int cnt = 3;

        List<File> filesBefore = new ArrayList<>();

        for (int i = 0; i < cnt; i++) {
            G.allGrids().forEach(ignite -> ignite.cache(DEFAULT_CACHE_NAME).get(0));

            LogListener lsnr = matches("Performance statistics writer rotated")
                .times(NODES_CNT)
                .build();

            listeningLog.registerListener(lsnr);

            List<File> files = statisticsFiles();

            files.removeAll(filesBefore);

            filesBefore.addAll(files);

            rotateCollectStatistics();

            assertTrue(waitForCondition(lsnr::check, TIMEOUT));

            checkFiles(performanceStatisticsFiles(files), NODES_CNT, NODES_CNT);

            if (i == 0)
                checkFiles(systemViewStatisticsFiles(files), NODES_CNT, 0);
        }

        stopCollectStatistics();

        checkFiles(performanceStatisticsFiles(statisticsFiles()), NODES_CNT * (cnt + 1), NODES_CNT * cnt);
        checkFiles(systemViewStatisticsFiles(statisticsFiles()), NODES_CNT, 0);
    }

    /** Checks files and operations count. */
    private void checkFiles(List<File> files, int expFiles, int expOps) throws Exception {
        AtomicInteger ops = new AtomicInteger();

        readFiles(files, new TestHandler() {
            @Override public void cacheOperation(UUID nodeId, OperationType type, int cacheId, long startTime,
                long duration) {
                ops.incrementAndGet();
            }
        });

        assertEquals(expFiles, files.size());
        assertEquals(expOps, ops.get());
    }
}
