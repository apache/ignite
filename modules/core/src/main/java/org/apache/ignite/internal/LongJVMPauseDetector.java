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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.logger.java.JavaLogger;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_STW_ALLOWED_DURATION_LIMIT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_STW_DETECTOR_SLEEP_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 *
 */
class LongJVMPauseDetector {
    /** Logger. */
    private static final IgniteLogger LOG = new JavaLogger();

    /** Started. */
    private static final AtomicBoolean started = new AtomicBoolean();

    /** Long pause count. */
    private static volatile long longPausesCount;

    /** Long pause total duration. */
    private static volatile long longPausesTotalDuration;

    /**
     * Starts worker if not started yet.
     */
    public static void start() {
        if (!started.compareAndSet(false, true)) {
            LOG.warning(LongJVMPauseDetector.class.getSimpleName() + " already started!");

            return;
        }

        final Thread worker = new Thread("jvm-pause-detector-worker") {
            private long prev = System.currentTimeMillis();

            private long longPausesCount;

            private long longPausesTotalDuration;

            @Override public void run() {
                final int timeout = getInteger(IGNITE_STW_DETECTOR_SLEEP_TIMEOUT, 10);
                final int threshold = getInteger(IGNITE_STW_ALLOWED_DURATION_LIMIT, 100);

                while (true) {
                    try {
                        Thread.sleep(timeout);

                        final long now = System.currentTimeMillis();
                        final long pause = now - timeout - prev;

                        prev = now;

                        if (pause >= threshold) {
                            LOG.warning("Possible too long JVM pause: " + pause + " milliseconds.");

                            longPausesCount(++longPausesCount);

                            longPausesTotalDuration(longPausesTotalDuration += pause);
                        }
                    }
                    catch (InterruptedException e) {
                        LOG.error(getName() + " has been interrupted", e);

                        break;
                    }
                }
            }
        };

        worker.setDaemon(true);
        worker.start();

        LOG.info(worker.getName() + " has been started.");
    }

    /**
     * @return Long JVM pauses count
     */
    static long longPausesCount() {
        return longPausesCount;
    }

    /**
     * @param val Value.
     */
    private static void longPausesCount(long val) {
        longPausesCount = val;
    }

    /**
     * @return Long JVM pauses total duration
     */
    static long longPausesTotalDuration() {
        return longPausesTotalDuration;
    }

    /**
     * @param val Value.
     */
    private static void longPausesTotalDuration(long val) {
        longPausesTotalDuration = val;
    }
}
