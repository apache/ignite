/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests if LongJVMPauseDetector starts properly.
 */
public class LongJVMPauseDetectorTest extends GridCommonAbstractTest {
    /** */
    private GridStringLogger strLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (strLog != null)
            cfg.setGridLogger(strLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJulMessage() throws Exception {
        this.strLog = new GridStringLogger(true);

        strLog.logLength(300000);

        startGrid(0);

        assertTrue(strLog.toString().contains("LongJVMPauseDetector was successfully started"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopWorkerThread() throws Exception {
        strLog = new GridStringLogger(true);

        strLog.logLength(300_000);

        startGrid(0);

        stopGrid(0);

        String log = strLog.toString();

        assertFalse(log.contains("jvm-pause-detector-worker has been interrupted."));
        assertTrue(log.contains("jvm-pause-detector-worker has been stopped."));
    }
}
