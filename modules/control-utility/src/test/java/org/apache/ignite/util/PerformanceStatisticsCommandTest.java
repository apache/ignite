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

package org.apache.ignite.util;

import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.performancestatistics.PerformanceStatisticsSubCommand;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandList.PERFORMANCE_STATISTICS;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.cleanPerformanceStatisticsDir;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.waitForStatisticsEnabled;
import static org.apache.ignite.internal.visor.performancestatistics.VisorPerformanceStatisticsTask.STATUS_DISABLED;
import static org.apache.ignite.internal.visor.performancestatistics.VisorPerformanceStatisticsTask.STATUS_ENABLED;

/** Tests {@link CommandList#PERFORMANCE_STATISTICS} command. */
public class PerformanceStatisticsCommandTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (crd.context().performanceStatistics().enabled()) {
            crd.context().performanceStatistics().stopCollectStatistics();

            waitForStatisticsEnabled(false);
        }

        cleanPerformanceStatisticsDir();

        assertFalse(crd.context().performanceStatistics().enabled());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        cleanPerformanceStatisticsDir();
    }

    /** @throws Exception If failed. */
    @Test
    public void testStart() throws Exception {
        int res = execute(PERFORMANCE_STATISTICS.text(), PerformanceStatisticsSubCommand.START.toString());

        assertEquals(EXIT_CODE_OK, res);

        waitForStatisticsEnabled(true);
    }

    /** @throws Exception If failed. */
    @Test
    public void testStop() throws Exception {
        crd.context().performanceStatistics().startCollectStatistics();

        waitForStatisticsEnabled(true);

        int res = execute(PERFORMANCE_STATISTICS.text(), PerformanceStatisticsSubCommand.STOP.toString());

        assertEquals(EXIT_CODE_OK, res);

        waitForStatisticsEnabled(false);
    }

    /** @throws Exception If failed. */
    @Test
    public void testStatus() throws Exception {
        int res = execute(PERFORMANCE_STATISTICS.text(), PerformanceStatisticsSubCommand.STATUS.toString());

        assertEquals(EXIT_CODE_OK, res);

        assertEquals(STATUS_DISABLED, lastOperationResult);

        crd.context().performanceStatistics().startCollectStatistics();

        waitForStatisticsEnabled(true);

        res = execute(PERFORMANCE_STATISTICS.text(), PerformanceStatisticsSubCommand.STATUS.toString());

        assertEquals(EXIT_CODE_OK, res);

        assertEquals(STATUS_ENABLED, lastOperationResult);
    }
}
