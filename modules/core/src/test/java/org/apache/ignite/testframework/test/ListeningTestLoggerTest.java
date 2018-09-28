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

package org.apache.ignite.testframework.test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Test.
 */
public class ListeningTestLoggerTest extends GridCommonAbstractTest {
    /** */
    private ListeningTestLogger log = new ListeningTestLogger(false, super.log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(log);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        log.clearListeners();
    }


    /**
     * @throws Exception If failed.
     */
    public void testIgniteVersionLogging() throws Exception {
        int gridCnt = 4;

        LogListener lsnr = LogListener.matches(IgniteVersionUtils.VER_STR).atLeast(gridCnt).build();

        log.registerListener(lsnr);

        try {
            startGridsMultiThreaded(gridCnt);

            lsnr.check();
        } finally {
            stopAllGrids();
        }
    }

    /**
     * Checks that re-register works fine.
     */
    public void testUnregister() {
        String msg = "catch me";

        LogListener lsnr1 = LogListener.matches(msg).times(1).build();
        LogListener lsnr2 = LogListener.matches(msg).times(2).build();

        log.registerListener(lsnr1);
        log.registerListener(lsnr2);

        log.info(msg);

        log.unregisterListener(lsnr1);

        log.info(msg);

        lsnr1.check();
        lsnr2.check();

        log.clearListeners();

        // Retry this steps to make sure that the state was reset.
        log.registerListener(lsnr1);
        log.registerListener(lsnr2);

        log.info(msg);

        log.unregisterListener(lsnr1);

        log.info(msg);

        lsnr1.check();
        lsnr2.check();

        // Ensures that listener will be re-registered only once.
        AtomicInteger cntr = new AtomicInteger();

        LogListener lsnr3 = LogListener.matches(m -> cntr.incrementAndGet() > 0).build();

        log.registerListener(lsnr3);
        log.registerListener(lsnr3);

        log.info("1");

        assertEquals(1, cntr.get());
    }

    /**
     * Check basic API.
     */
    @SuppressWarnings("ThrowableNotThrown")
    public void testBasicApi() {
        String errMsg = "Word started with \"a\" not found.";

        LogListener lsnr = LogListener.matches(Pattern.compile("a[a-z]+")).orError(errMsg)
            .andMatches("Exception message.").andMatches(".java:").build();

        log.registerListener(lsnr);

        log.info("Something new.");

        assertThrows(lsnr::check, AssertionError.class, errMsg);

        log.error("There was an error.", new RuntimeException("Exception message."));

        lsnr.check();
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    public void testPredicateExceptions() {
        LogListener lsnr = LogListener.matches(msg -> {
            assertFalse(msg.contains("Target"));

            return true;
        }).build();

        log.registerListener(lsnr);

        log.info("Ignored message.");
        log.info("Target message.");

        assertThrows(lsnr::check, AssertionError.class, null);

        // Check custom exception.
        LogListener lsnr2 = LogListener.matches(msg -> {
            throw new IllegalStateException("Illegal state");
        }).orError("blah-blah").build();

        log.registerListener(lsnr2);

        log.info("1");
        log.info("2");

        assertThrows(lsnr2::check, IllegalStateException.class, "Illegal state");
    }

    /**
     * Validates listener range definition.
     */
    @SuppressWarnings("ThrowableNotThrown")
    public void testRange() {
        String msg = "test message";

        LogListener lsnr2 = LogListener.matches(msg).times(2).build();
        LogListener lsnr23 = LogListener.matches(msg).atLeast(2).atMost(3).build();

        log.registerListener(lsnr2);
        log.registerListener(lsnr23);

        log.info(msg);
        log.info(msg);

        lsnr2.check();
        lsnr23.check();

        log.info(msg);

        assertThrows(lsnr2::check, AssertionError.class, null);

        lsnr23.check();

        log.info(msg);

        assertThrows(lsnr23::check, AssertionError.class, null);

        // Check that susbtring was not found in log messages.
        LogListener notPresent = LogListener.matches(msg).times(0).build();

        log.registerListener(notPresent);

        log.info("1");

        notPresent.check();

        log.info(msg);

        assertThrows(notPresent::check, AssertionError.class, null);

        // Check that the substring is found at least twice.
        LogListener atLeast2 = LogListener.matches(msg).atLeast(2).build();

        log.registerListener(atLeast2);

        log.info(msg);

        assertThrows(atLeast2::check, AssertionError.class, null);

        log.info(msg);

        atLeast2.check();

        // Check that the substring is found no more than twice.
        LogListener atMost2 = LogListener.matches(msg).atMost(2).build();

        log.registerListener(atMost2);

        atMost2.check();

        log.info(msg);
        log.info(msg);

        atMost2.check();

        log.info(msg);

        assertThrows(atMost2::check, AssertionError.class, null);

        // Check that only last value is taken into account.
        LogListener atMost3 = LogListener.matches(msg).times(1).times(2).atMost(3).build();

        log.registerListener(atMost3);

        for (int i = 0; i < 6; i++) {
            if (i < 4)
                atMost3.check();
            else
                assertThrows(atMost3::check, AssertionError.class, null);

            log.info(msg);
        }

        LogListener lsnr4 = LogListener.matches(msg).atLeast(2).atMost(3).times(4).build();

        log.registerListener(lsnr4);

        for (int i = 1; i < 6; i++) {
            log.info(msg);

            if (i == 4)
                lsnr4.check();
            else
                assertThrows(lsnr4::check, AssertionError.class, null);
        }
    }

    /**
     * CHeck thread safety.
     *
     * @throws Exception If failed.
     */
    public void testMultithreaded() throws Exception {
        int iterCnt = 50_000;
        int threadCnt = 6;
        int total = threadCnt * iterCnt;

        ListeningTestLogger log = new ListeningTestLogger();

        LogListener lsnr = LogListener.matches("abba").times(total)
            .andMatches("ab").times(total)
            .andMatches("ba").times(total)
            .build();

        log.registerListener(lsnr);

        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < iterCnt; i++)
                log.info("It is the abba message.");
        }, threadCnt, "thread-");

        lsnr.check();
    }

    /**
     * Check "echo" logger.
     */
    public void testEchoLogger() {
        StringBuilder buf = new StringBuilder();

        IgniteLogger echo = new NullLogger() {
            @Override public void trace(String msg) {
                buf.append(msg);
            }

            @Override public void debug(String msg) {
                buf.append(msg);
            }

            @Override public void info(String msg) {
                buf.append(msg);
            }

            @Override public void warning(String msg, Throwable t) {
                buf.append(msg);
            }

            @Override public void error(String msg, Throwable t) {
                buf.append(msg);
            }
        };

        ListeningTestLogger log = new ListeningTestLogger(true, echo);

        log.error("1");
        log.warning("2");
        log.info("3");
        log.debug("4");
        log.trace("5");

        assertEquals("12345", buf.toString());
    }

    /**
     * Checks whether callable throws expected exception or not.
     *
     * @param runnable Runnable..
     * @param cls Exception class.
     * @param msg Exception message (optional). If provided exception message
     *      and this message should be equal.
     */
    @SuppressWarnings("ThrowableNotThrown")
    private void assertThrows(
        Runnable runnable,
        Class<? extends Throwable> cls,
        @Nullable String msg
    ) throws AssertionError {
        GridTestUtils.assertThrows(log(), () -> {
            runnable.run();

            return null;
        }, cls, msg);
    }
}