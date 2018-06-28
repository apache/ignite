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

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.testframework.GridRegexpLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class GridRegexpLoggerTest {
    /** Logger. */
    public static GridRegexpLogger log;

    /** Delegate logger output. */
    public static StringBuffer delegateOutput;

    /** Use delegate logger. */
    public static boolean useDelegateLog;

    /**
     *
     */
    @BeforeClass
    public static void beforeClass(){
        log = new GridRegexpLogger();
    }

    /**
     *
     */
    @After
    public void tearDown() {
        log.clearListeners();

        if (useDelegateLog)
            delegateOutput = new StringBuffer();
    }

    /**
     *
     */
    @Test
    public void testBasicLogListening(){
        AtomicBoolean stopLogging = new AtomicBoolean(FALSE);

        log.listen("a[a-z]+", o -> stopLogging.set(TRUE));

        log.debug("a");

        assertFalse(stopLogging.get());

        log.debug("ab");

        assertTrue(stopLogging.get());

        if (useDelegateLog)
            assertEquals("aab", delegateOutput.toString());
    }

    /**
     *
     */
    @Test
    public void testLogWait() throws InterruptedException {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

        executor.scheduleWithFixedDelay(() -> log.debug("ab"), 0, 300, MILLISECONDS);

        assertTrue(log.waitFor("a[a-z]+", SECONDS.toMillis(1)));

        executor.shutdownNow();

        if (useDelegateLog)
            assertTrue(delegateOutput.toString().contains("ab"));
    }

    /**
     *
     */
    @Test
    public void testBrokenListener(){
        log.listen("a[a-z]+", s -> {throw new RuntimeException();});

        log.debug("a");

        GridTestUtils.assertThrowsWithCause(p -> log.debug("ab"), null, RuntimeException.class);
    }

    /**
     *
     */
    @Test
    public void testSequentialNotifying(){
        AtomicInteger cntr = new AtomicInteger(0);

        log.listenDebug("a[a-z]+", s -> cntr.compareAndSet(0, 1));
        log.listenDebug(".*", s -> cntr.compareAndSet(1, 2));
        log.listen("a[a-z]+", s -> cntr.compareAndSet(2, 3));
        log.listenDebug("ab", s -> cntr.compareAndSet(3, 4));
        log.listen("a[a-z]+", s -> cntr.compareAndSet(4, 5));

        log.debug("ab");

        assertEquals(5, cntr.get());
    }
}