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
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.logger.java.JavaLogger;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_PRECISION;
import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 * Class for detection of long JVM pauses.
 * It has a worker thread, which wakes up in cycle every {@code PRECISION} (default is 50) milliseconds,
 * and monitors a time values between awakenings. If worker pause exceeds the expected value more than {@code THRESHOLD}
 * default is 500), the difference is considered as JVM pause, and event of long JVM pause is registered.
 * The values of {@code PRECISION}, {@code THRESHOLD} and {@code EVT_CNT} (event window size, default is 20) can be
 * configured in system or environment properties IGNITE_JVM_PAUSE_DETECTOR_PRECISION,
 * IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD and IGNITE_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT accordingly.
 */
class LongJVMPauseDetector {
    /** Logger. */
    private static final IgniteLogger LOG = new JavaLogger();

    /** Worker reference. */
    private static final AtomicReference<Thread> workerRef = new AtomicReference<>();

    /** Precision. */
    private static final int PRECISION = getInteger(IGNITE_JVM_PAUSE_DETECTOR_PRECISION, 50);

    /** Threshold. */
    private static final int THRESHOLD = getInteger(IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD, 500);

    /** Event count. */
    private static final int EVT_CNT = getInteger(IGNITE_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT, 20);

    /** Long pause count. */
    private static long longPausesCnt;

    /** Long pause total duration. */
    private static long longPausesTotalDuration;

    /** Long pauses timestamps. */
    private static final long[] longPausesTimestamps = new long[EVT_CNT];

    /** Long pauses durations. */
    private static final long[] longPausesDurations = new long[EVT_CNT];

    /**
     * Starts worker if not started yet.
     */
    public static void start() {
        final Thread worker = new Thread("jvm-pause-detector-worker") {
            private long prev = System.currentTimeMillis();

            @Override public void run() {
                if (LOG.isDebugEnabled())
                    LOG.debug(getName() + " has been started.");

                while (true) {
                    try {
                        Thread.sleep(PRECISION);

                        final long now = System.currentTimeMillis();
                        final long pause = now - PRECISION - prev;

                        prev = now;

                        if (pause >= THRESHOLD) {
                            LOG.warning("Possible too long JVM pause: " + pause + " milliseconds.");

                            synchronized (LongJVMPauseDetector.class) {
                                final int next = (int)(longPausesCnt % EVT_CNT);

                                longPausesCnt++;

                                longPausesTotalDuration += pause;

                                longPausesTimestamps[next] = now;

                                longPausesDurations[next] = pause;
                            }
                        }
                    }
                    catch (InterruptedException e) {
                        LOG.error(getName() + " has been interrupted", e);

                        break;
                    }
                }
            }
        };

        if (!workerRef.compareAndSet(null, worker)) {
            LOG.warning(LongJVMPauseDetector.class.getSimpleName() + " already started!");

            return;
        }

        worker.setDaemon(true);
        worker.start();
    }

    /**
     * Stops the worker if one is created and running.
     */
    public static void stop() {
        final Thread worker = workerRef.getAndSet(null);

        if (worker != null && worker.isAlive() && !worker.isInterrupted())
            worker.interrupt();
    }

    /**
     * @return Long JVM pauses count.
     */
    synchronized static long longPausesCount() {
        return longPausesCnt;
    }

    /**
     * @return Long JVM pauses total duration.
     */
    synchronized static long longPausesTotalDuration() {
        return longPausesTotalDuration;
    }

    /**
     * @return Last long JVM pause events.
     */
    synchronized static Map<Long, Long> longPauseEvents() {
        final Map<Long, Long> evts = new TreeMap<>();

        for (int i = 0; i < longPausesTimestamps.length && longPausesTimestamps[i] != 0; i++)
            evts.put(longPausesTimestamps[i], longPausesDurations[i]);

        return evts;
    }
}
