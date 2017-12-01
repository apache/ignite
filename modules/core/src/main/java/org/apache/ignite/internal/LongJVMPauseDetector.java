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

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.logger.java.JavaLogger;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_PRECISION;
import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 * Class for detection of long JVM pauses (see <a href="https://issues.apache.org/jira/browse/IGNITE-6171">IGNITE-6171</a>).
 */
class LongJVMPauseDetector {
    /** Logger. */
    private static final IgniteLogger LOG = new JavaLogger();

    /** Started. */
    private static final AtomicBoolean started = new AtomicBoolean();

    /** Long pause count. */
    private static long longPausesCnt;

    /** Long pause total duration. */
    private static long longPausesTotalDuration;

    /** Long pauses timestamps. */
    private static long[] longPausesTimestamps;

    /** Long pauses durations. */
    private static long[] longPausesDurations;

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

            @Override public void run() {
                if (LOG.isDebugEnabled())
                    LOG.debug(getName() + " has been started.");

                final int precision = getInteger(IGNITE_JVM_PAUSE_DETECTOR_PRECISION, 50);
                final int threshold = getInteger(IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD, 500);
                final int evtCnt = getInteger(IGNITE_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT, 20);

                synchronized (LongJVMPauseDetector.class) {
                    longPausesTimestamps = new long[evtCnt];
                    longPausesDurations = new long[evtCnt];
                }

                while (true) {
                    try {
                        Thread.sleep(precision);

                        final long now = System.currentTimeMillis();
                        final long pause = now - precision - prev;

                        prev = now;

                        if (pause >= threshold) {
                            LOG.warning("Possible too long JVM pause: " + pause + " milliseconds.");

                            synchronized (LongJVMPauseDetector.class) {
                                final int next = (int)(longPausesCnt % evtCnt);

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

        worker.setDaemon(true);
        worker.start();
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
        if (longPausesTimestamps == null)
            return Collections.emptyMap();

        final Map<Long, Long> evts = new TreeMap<>();

        for (int i = 0; i < longPausesTimestamps.length && longPausesTimestamps[i] != 0; i++)
            evts.put(longPausesTimestamps[i], longPausesDurations[i]);

        return evts;
    }
}
