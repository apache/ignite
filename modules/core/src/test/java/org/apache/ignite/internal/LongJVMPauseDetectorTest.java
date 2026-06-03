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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
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

    /**
     * This test will create, check logic,
     * check report gethering and gracefull shutdown
     */
    @Test
    public void testFullCycle() throws InterruptedException {
        long[] monotonicTimes = new long[] {
            0, // init field
            100, // first renew of last wake-up time
            100, // check we didn't wake up spuriously
            100, // we need to get real wait time, so now time is
            50_000_100, // so we waited 50 ms, and previous time is 100 ns, so it's that time now
            550_000_100}; // something happend! Waited for 500 ms (equals to 500ms + previous nanotime)!
        CountDownLatch cntDownLatch = new CountDownLatch(7);
        LongJVMPauseDetector longJVMPauseDetector = getLongJVMPauseDetector(monotonicTimes, cntDownLatch);
        longJVMPauseDetector.start();
        assertTrue(cntDownLatch.await(10, TimeUnit.SECONDS));
        assertEquals(500, longJVMPauseDetector.longPausesTotalDuration());
        assertEquals(1, longJVMPauseDetector.longPausesCount());
        Map<Long, Long> longPauseEvts = longJVMPauseDetector.longPauseEvents();
        assertTrue(longPauseEvts.containsValue(500L));
        Optional<String> spottedPausesExplain = longJVMPauseDetector.getTotalSpottedPausesExplain(100);
        assertTrue(spottedPausesExplain.isPresent());
        assertTrue(spottedPausesExplain.get().matches("Pause detecor spotted 1 pauses: \\[500 ms at \\d+;]. Each with precision 50 ms"));
        longJVMPauseDetector.stop();
    }

    /**
     * @param monotonicTimes Monotonic times. Answers for method
     *                       {@link org.apache.ignite.internal.LongJVMPauseDetector#getMonotonicTimeNanos()}
     * @param cntDownLatch Count down latch.
     */
    private @NotNull LongJVMPauseDetector getLongJVMPauseDetector(long[] monotonicTimes, CountDownLatch cntDownLatch) {
        return new LongJVMPauseDetector("test-instance", listeningTestLogger) {
            private int visitCounter;
            /** {@inheritDoc} */
            @Override protected long getMonotonicTimeNanos() {
                synchronized (this) {
                    visitCounter++;
                    if (visitCounter - 1 >= monotonicTimes.length) {
                        cntDownLatch.countDown();
                        while (!Thread.currentThread().isInterrupted()) {
                            try {
                                wait(Long.MAX_VALUE);
                            }
                            catch (InterruptedException ignored) {
                                // do nothing
                            }
                        }
                        return 0;
                    }
                    cntDownLatch.countDown();
                    return monotonicTimes[visitCounter - 1];
                }
            }
        };
    }
}
