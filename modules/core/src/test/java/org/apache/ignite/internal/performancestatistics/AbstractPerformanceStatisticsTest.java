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

package org.apache.ignite.internal.performancestatistics;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.performancestatistics.TestFilePerformanceStatisticsReader.readToLog;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.PERFORMANCE_STAT_DIR;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Ignite performance statistics abstract test.
 */
public abstract class AbstractPerformanceStatisticsTest extends GridCommonAbstractTest {
    /** */
    public static final long TIMEOUT = 30_000;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), PERFORMANCE_STAT_DIR, false));
    }

    /** Starts collecting performance statistics. */
    public static void startCollectStatistics() throws Exception {
        List<Ignite> grids = G.allGrids();

        assertFalse(grids.isEmpty());

        IgniteEx ignite = (IgniteEx)grids.get(0);

        ignite.context().performanceStatistics().startCollectStatistics();

        waitForStatisticsEnabled(true);
    }

    /** Stops collecting performance statistics and checks listeners on all grids. */
    public static void stopCollectStatisticsAndCheck(LogListener... lsnrs) throws Exception {
        List<Ignite> grids = G.allGrids();

        assertFalse(grids.isEmpty());

        IgniteEx ignite = (IgniteEx)grids.get(0);

        ignite.context().performanceStatistics().stopCollectStatistics();

        waitForStatisticsEnabled(false);

        ListeningTestLogger log = new ListeningTestLogger(GridAbstractTest.log);

        for (LogListener lsnr : lsnrs)
            log.registerListener(lsnr);

        readToLog(log);

        for (LogListener lsnr : lsnrs)
            assertTrue(lsnr.check());
    }

    /** Wait for statistics enabled/disabled. */
    public static void waitForStatisticsEnabled(boolean enabled) throws IgniteInterruptedCheckedException {
        assertTrue(waitForCondition(() -> {
            List<Ignite> grids = G.allGrids();

            for (Ignite grid : grids)
                if (enabled != ((IgniteEx)grid).context().performanceStatistics().enabled())
                    return false;

            return true;
        }, TIMEOUT));
    }
}
