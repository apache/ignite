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

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
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
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ListeningTestLoggerTest extends GridCommonAbstractTest {
    /** */
    private final ListeningTestLogger log = new ListeningTestLogger(false, super.log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(log);

        return cfg;
    }

    /**
     * Basic example of using listening logger - checks that all running instances of Ignite print product version.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteVersionLogging() throws Exception {
        int gridCnt = 4;

        LogListener lsnr = LogListener.matches(IgniteVersionUtils.VER_STR).atLeast(gridCnt).build();

        log.registerListener(lsnr);

        try {
            startGridsMultiThreaded(gridCnt);

            assertTrue(lsnr.check());
        } finally {
            stopAllGrids();
        }
    }

    /**
     * Checks that re-register works fine.
     */
    @Test
    public void testUnregister() {
        String msg = "catch me";

        LogListener lsnr1 = LogListener.matches(msg).times(1).build();
        LogListener lsnr2 = LogListener.matches(msg).times(2).build();

        log.registerListener(lsnr1);
        log.registerListener(lsnr2);

        log.info(msg);

        log.unregisterListener(lsnr1);

        log.info(msg);

        assertTrue(lsnr1.check());
        assertTrue(lsnr2.check());

        // Repeat these steps to ensure that the state is cleared during registration.
        log.registerListener(lsnr1);
        log.registerListener(lsnr2);

        log.info(msg);

        log.unregisterListener(lsnr1);

        log.info(msg);

        assertTrue(lsnr1.check());
        assertTrue(lsnr2.check());
    }

    /**
     * Ensures that listener will be re-registered only once.
     */
    @Test
    public void testRegister() {
        AtomicInteger cntr = new AtomicInteger();

        LogListener lsnr3 = LogListener.matches(m -> cntr.incrementAndGet() > 0).build();

        log.registerListener(lsnr3);
        log.registerListener(lsnr3);

        log.info("1");

        assertEquals(1, cntr.get());
    }

    /**
     * Checks basic API.
     */
    @Test
    public void testBasicApi() {
        LogListener lsnr = LogListener.matches(Pattern.compile("a[a-z]+"))
            .andMatches("Exception message.").andMatches(".java:").build();

        log.registerListener(lsnr);

        log.info("Something new.");

        assertFalse(lsnr.check());

        log.error("There was an error.", new RuntimeException("Exception message."));

        assertTrue(lsnr.check());
    }

    /**
     * Checks blank lines matching.
     */
    @Test
    public void testEmptyLine() {
        LogListener emptyLineLsnr = LogListener.matches("").build();

        log.registerListener(emptyLineLsnr);

        log.info("");

        assertTrue(emptyLineLsnr.check());
    }

    /** */
    @Test
    public void testPredicateExceptions() {
        LogListener lsnr = LogListener.matches(msg -> {
            assertFalse(msg.contains("Target"));

            return true;
        }).build();

        log.registerListener(lsnr);

        log.info("Ignored message.");
        log.info("Target message.");

        assertThrowsWithCause((Callable<Object>)lsnr::check, AssertionError.class);

        // Check custom exception.
        LogListener lsnr2 = LogListener.matches(msg -> {
            throw new IllegalStateException("Illegal state");
        }).build();

        log.registerListener(lsnr2);

        log.info("1");
        log.info("2");

        assertThrowsWithCause((Callable<Object>)lsnr2::check, IllegalStateException.class);
    }

    /**
     * Validates listener range definition.
     */
    @Test
    public void testRange() {
        String msg = "range";

        LogListener lsnr2 = LogListener.matches(msg).times(2).build();
        LogListener lsnr2_3 = LogListener.matches(msg).atLeast(2).atMost(3).build();

        log.registerListener(lsnr2);
        log.registerListener(lsnr2_3);

        log.info(msg);
        log.info(msg);

        assertTrue(lsnr2.check());
        assertTrue(lsnr2_3.check());

        log.info(msg);

        assertFalse(lsnr2.check());

        assertTrue(lsnr2_3.check());

        log.info(msg);

        assertFalse(lsnr2_3.check());
    }

    /**
     * Checks that substring was not found in the log messages.
     */
    @Test
    public void testNotPresent() {
        String msg = "vacuum";

        LogListener notPresent = LogListener.matches(msg).times(0).build();

        log.registerListener(notPresent);

        log.info("1");

        assertTrue(notPresent.check());

        log.info(msg);

        assertFalse(notPresent.check());
    }

    /**
     * Checks that the substring is found at least twice.
     */
    @Test
    public void testAtLeast() {
        String msg = "at least";

        LogListener atLeast2 = LogListener.matches(msg).atLeast(2).build();

        log.registerListener(atLeast2);

        log.info(msg);

        assertFalse(atLeast2.check());

        log.info(msg);

        assertTrue(atLeast2.check());
    }

    /**
     * Checks that the substring is found no more than twice.
     */
    @Test
    public void testAtMost() {
        String msg = "at most";

        LogListener atMost2 = LogListener.matches(msg).atMost(2).build();

        log.registerListener(atMost2);

        assertTrue(atMost2.check());

        log.info(msg);
        log.info(msg);

        assertTrue(atMost2.check());

        log.info(msg);

        assertFalse(atMost2.check());
    }

    /**
     * Checks that only last value is taken into account.
     */
    @Test
    public void testMultiRange() {
        String msg = "multi range";

        LogListener atMost3 = LogListener.matches(msg).times(1).times(2).atMost(3).build();

        log.registerListener(atMost3);

        for (int i = 0; i < 6; i++) {
            if (i < 4)
                assertTrue(atMost3.check());
            else
                assertFalse(atMost3.check());

            log.info(msg);
        }

        LogListener lsnr4 = LogListener.matches(msg).atLeast(2).atMost(3).times(4).build();

        log.registerListener(lsnr4);

        for (int i = 1; i < 6; i++) {
            log.info(msg);

            if (i == 4)
                assertTrue(lsnr4.check());
            else
                assertFalse(lsnr4.check());
        }
    }

    /**
     * Checks that matches are counted for each message.
     */
    @Test
    public void testMatchesPerMessage() {
        LogListener lsnr = LogListener.matches("aa").times(4).build();

        log.registerListener(lsnr);

        log.info("aabaab");
        log.info("abaaab");

        assertTrue(lsnr.check());

        LogListener newLineLsnr = LogListener.matches("\n").times(5).build();

        log.registerListener(newLineLsnr);

        log.info("\n1\n2\n\n3\n");

        assertTrue(newLineLsnr.check());

        LogListener regexpLsnr = LogListener.matches(Pattern.compile("(?i)hi|hello")).times(3).build();

        log.registerListener(regexpLsnr);

        log.info("Hi! Hello!");
        log.info("Hi folks");

        assertTrue(regexpLsnr.check());
    }

    /**
     * Check thread safety.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreaded() throws Exception {
        int iterCnt = 50_000;
        int threadCnt = 6;
        int total = threadCnt * iterCnt;
        int rndNum = ThreadLocalRandom.current().nextInt(iterCnt);

        ListeningTestLogger log = new ListeningTestLogger();

        LogListener lsnr = LogListener.matches("abba").times(total)
            .andMatches(Pattern.compile("(?i)abba")).times(total * 2)
            .andMatches("ab").times(total)
            .andMatches("ba").times(total)
            .build();

        LogListener mtLsnr = LogListener.matches("abba").build();

        log.registerListener(lsnr);

        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < iterCnt; i++) {
                if (rndNum == i)
                    log.registerListener(mtLsnr);

                log.info("It is the abba(ABBA) message.");
            }
        }, threadCnt, "test-listening-log");

        assertTrue(lsnr.check());
        assertTrue(mtLsnr.check());
    }

    /**
     * Check "echo" logger.
     */
    @Test
    public void testEchoLogger() {
        IgniteLogger echo = new StringLogger();

        ListeningTestLogger log = new ListeningTestLogger(true, echo);

        log.error("1");
        log.warning("2");
        log.info("3");
        log.debug("4");
        log.trace("5");

        assertEquals("12345", echo.toString());
    }

    /** */
    private static class StringLogger extends NullLogger {
        /** */
        private final StringBuilder buf = new StringBuilder();

        /** {@inheritDoc} */
        @Override public void trace(String msg) {
            buf.append(msg);
        }

        /** {@inheritDoc} */
        @Override public void debug(String msg) {
            buf.append(msg);
        }

        /** {@inheritDoc} */
        @Override public void info(String msg) {
            buf.append(msg);
        }

        /** {@inheritDoc} */
        @Override public void warning(String msg, Throwable t) {
            buf.append(msg);
        }

        /** {@inheritDoc} */
        @Override public void error(String msg, Throwable t) {
            buf.append(msg);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return buf.toString();
        }
    }
}
