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
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.commandline.CommandList.PERFORMANCE_STATISTICS;
import static org.apache.ignite.internal.commandline.performancestatistics.PerformanceStatisticsSubCommand.ROTATE;
import static org.apache.ignite.internal.commandline.performancestatistics.PerformanceStatisticsSubCommand.START;
import static org.apache.ignite.internal.commandline.performancestatistics.PerformanceStatisticsSubCommand.STATUS;
import static org.apache.ignite.internal.commandline.performancestatistics.PerformanceStatisticsSubCommand.STOP;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.TIMEOUT;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.cleanPerformanceStatisticsDir;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.statisticsFiles;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.waitForStatisticsEnabled;
import static org.apache.ignite.internal.visor.performancestatistics.VisorPerformanceStatisticsTask.STATUS_DISABLED;
import static org.apache.ignite.internal.visor.performancestatistics.VisorPerformanceStatisticsTask.STATUS_ENABLED;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

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
    public void testCommands() throws Exception {
        int res = execute(PERFORMANCE_STATISTICS.text(), STATUS.toString());

        assertEquals(EXIT_CODE_OK, res);
        assertEquals(STATUS_DISABLED, lastOperationResult);

        res = execute(PERFORMANCE_STATISTICS.text(), ROTATE.toString());

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, res);
        assertEquals(null, lastOperationResult);

        assertEquals(0, statisticsFiles().size());

        res = execute(PERFORMANCE_STATISTICS.text(), START.toString());

        assertEquals(EXIT_CODE_OK, res);

        waitForStatisticsEnabled(true);

        res = execute(PERFORMANCE_STATISTICS.text(), STATUS.toString());

        assertEquals(EXIT_CODE_OK, res);
        assertEquals(STATUS_ENABLED, lastOperationResult);

        res = execute(PERFORMANCE_STATISTICS.text(), ROTATE.toString());

        assertEquals(EXIT_CODE_OK, res);
        assertEquals("Rotated.", lastOperationResult);

        assertTrue(waitForCondition(() -> {
            try {
                return statisticsFiles().size() == G.allGrids().size() * 2;
            }
            catch (Exception e) {
                fail();

                return false;
            }
        }, TIMEOUT));

        res = execute(PERFORMANCE_STATISTICS.text(), STATUS.toString());

        assertEquals(EXIT_CODE_OK, res);
        assertEquals(STATUS_ENABLED, lastOperationResult);

        res = execute(PERFORMANCE_STATISTICS.text(), STOP.toString());

        assertEquals(EXIT_CODE_OK, res);

        waitForStatisticsEnabled(false);

        res = execute(PERFORMANCE_STATISTICS.text(), STATUS.toString());

        assertEquals(EXIT_CODE_OK, res);
        assertEquals(STATUS_DISABLED, lastOperationResult);
    }

    /** @throws Exception If failed. */
    @Test
    public void testStartAlreadyStarted() throws Exception {
        int res = execute(PERFORMANCE_STATISTICS.text(), START.toString());

        assertEquals(EXIT_CODE_OK, res);

        waitForStatisticsEnabled(true);

        res = execute(PERFORMANCE_STATISTICS.text(), START.toString());

        assertEquals(EXIT_CODE_OK, res);
    }

    /** */
    @Test
    public void testStopAlreadyStopped() {
        int res = execute(PERFORMANCE_STATISTICS.text(), STOP.toString());

        assertEquals(EXIT_CODE_OK, res);
    }
}
