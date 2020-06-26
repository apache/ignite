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
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.performancestatistics.TestFilePerformanceStatisticsReader.readToLog;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatistics.PERFORMANCE_STAT_DIR;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatistics.statisticsFile;

/**
 * Ignite performance statistics abstract test.
 */
public abstract class AbstractPerformanceStatisticsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), PERFORMANCE_STAT_DIR, false));
    }

    /** Starts performance statistics. */
    public static void startStatistics() throws Exception {
        List<Ignite> grids = G.allGrids();

        assertFalse(grids.isEmpty());

        IgniteEx ignite = (IgniteEx)grids.get(0);

        ignite.context().performanceStatistics().startStatistics().get();
    }

    /** Stops performance statistics and checks listeners on all grids. */
    public static void stopStatisticsAndCheck(LogListener... lsnrs) throws Exception {
        List<Ignite> grids = G.allGrids();

        assertFalse(grids.isEmpty());

        IgniteEx ignite = (IgniteEx)grids.get(0);

        ignite.context().performanceStatistics().stopStatistics().get();

        for (Ignite grid : grids)
            readToLog(statisticsFile(((IgniteEx)grid).context()).toPath(), grid.log());

        for (LogListener lsnr : lsnrs)
            assertTrue(lsnr.check());
    }
}
