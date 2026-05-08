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

package org.apache.ignite.internal;

import java.util.regex.Pattern;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests if LongJVMPauseDetector starts properly.
 */
public class LongJVMPauseDetectorTest extends GridCommonAbstractTest {
    /** */
    private static ListeningTestLogger listeningTestLogger;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setGridLogger(listeningTestLogger);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        setLoggerDebugLevel();

        listeningTestLogger = new ListeningTestLogger(log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJulMessage() throws Exception {
        // We should discard ring check latency on server node.
        LogListener lsnr = LogListener.matches("LongJVMPauseDetector was successfully started").times(1).build();

        listeningTestLogger.registerListener(lsnr);

        startGrid(0);

        assertTrue(lsnr.check());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopWorkerThread() throws Exception {
        LogListener stopLsnr = LogListener.matches(Pattern.compile("jvm-pause-detector-worker-.* has been stopped\\.")).times(1).build();
        LogListener interruptLsnr = LogListener.matches(Pattern.compile("jvm-pause-detector-worker-.* has been interrupted\\.")).build();

        listeningTestLogger.registerListener(stopLsnr);
        listeningTestLogger.registerListener(interruptLsnr);

        startGrid(0);
        stopGrid(0);

        assertFalse(interruptLsnr.check());
        assertTrue(stopLsnr.check());
    }
}
