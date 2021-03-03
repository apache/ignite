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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.ClientType.CLIENT;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.ClientType.SERVER;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Performance statistics file rotation tests.
 */
@RunWith(Parameterized.class)
public class PerformanceStatisticsRotateFileTest extends AbstractPerformanceStatisticsTest {
    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** Client type to run operations from. */
    @Parameterized.Parameter
    public ClientType clientType;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "clientType={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {{SERVER}, {CLIENT}});
    }

    /** Ignite. */
    private static IgniteEx srv;

    /** Listener test logger. */
    private static final ListeningTestLogger listeningTestLog = new ListeningTestLogger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());
        cfg.setGridLogger(listeningTestLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        srv = startGrid(NODES_CNT - 1);

        startClientGrid(NODES_CNT);
    }

    /** @throws Exception If failed. */
    @Test
    public void testRotateFile() throws Exception {
        int cnt = 5;

        assertThrows(null, AbstractPerformanceStatisticsTest::rotateCollectStatistics, IgniteException.class,
            "Performance statistics collection not started.");

        startCollectStatistics();

        rotateCollectStatistics();

        assertTrue(awaitRotateFile());

        AtomicInteger ops = new AtomicInteger();

        TestHandler hnd = new TestHandler() {
            @Override public void cacheOperation(UUID nodeId, OperationType type, int cacheId, long startTime,
                long duration) {
                ops.incrementAndGet();
            }
        };

        for (int i = 1; i <= cnt; i++) {
            srv.cache(DEFAULT_CACHE_NAME).get(0);

            rotateCollectStatistics();

            assertTrue(awaitRotateFile());

            String sfx = "-" + i + ".prf";

            List<File> files = statisticsFiles().stream()
                .filter(f -> f.getName().endsWith(sfx))
                .collect(Collectors.toList());

            readFiles(files, hnd);

            assertEquals(NODES_CNT, files.size());

            assertEquals(i, ops.get());
        }

        stopCollectStatistics();
    }

    /**
     * Awaiting for the performance statistics file to rotated.
     */
    private boolean awaitRotateFile() throws IgniteInterruptedCheckedException {
        LogListener logLsnr = LogListener.matches("Performance statistics writer rotated.").build();

        listeningTestLog.registerListener(logLsnr);

        return waitForCondition(logLsnr::check, TIMEOUT);
    }
}
