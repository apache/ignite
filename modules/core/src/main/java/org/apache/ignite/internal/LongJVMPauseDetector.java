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
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_DISABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_PRECISION;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 * Class for detection of long JVM pauses.
 * It has a worker thread, which wakes up in cycle every {@code PRECISION} (default is 50) milliseconds,
 * and monitors a time values between awakenings. If worker pause exceeds the expected value more than {@code THRESHOLD}
 * default is 500), the difference is considered as JVM pause, most likely STW, and event of long JVM pause is registered.
 * The values of {@code PRECISION}, {@code THRESHOLD} and {@code EVT_CNT} (event window size, default is 20) can be
 * configured in system or environment properties IGNITE_JVM_PAUSE_DETECTOR_PRECISION,
 * IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD and IGNITE_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT accordingly.
 */
public class LongJVMPauseDetector {
    /** Ignite JVM pause detector threshold default value. */
    public static final int DEFAULT_JVM_PAUSE_DETECTOR_THRESHOLD = 500;

    /** @see IgniteSystemProperties#IGNITE_JVM_PAUSE_DETECTOR_PRECISION */
    public static final int DFLT_JVM_PAUSE_DETECTOR_PRECISION = 50;

    /** @see IgniteSystemProperties#IGNITE_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT */
    public static final int DFLT_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT = 20;

    /** Precision. */
    private static final long PRECISION =
        getInteger(IGNITE_JVM_PAUSE_DETECTOR_PRECISION, DFLT_JVM_PAUSE_DETECTOR_PRECISION);

    /** Precision. */
    private static final long PRECISION_NANOS = PRECISION * 1_000_000L;

    /** Threshold. */
    private static final int THRESHOLD =
        getInteger(IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD, DEFAULT_JVM_PAUSE_DETECTOR_THRESHOLD);

    /** Event count. */
    private static final int EVT_CNT =
        getInteger(IGNITE_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT, DFLT_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT);

    /** Disabled flag. */
    private static final boolean DISABLED = getBoolean(IGNITE_JVM_PAUSE_DETECTOR_DISABLED);

    /** */
    private final String igniteInstanceName;

    /** Logger. */
    private final IgniteLogger log;

    /** Worker reference. */
    private final AtomicReference<Thread> workerRef = new AtomicReference<>();

    /** Long pause count. */
    private long longPausesCnt;

    /** Long pause total duration. */
    private long longPausesTotalDurationNanos;

    /** Last detector's wake up time. */
    private long lastWakeUpTimeNanos = getMonotonicTimeNanos();

    /** Long pauses timestamps. */
    @GridToStringInclude
    private final long[] longPausesTimestamps = new long[EVT_CNT];

    /** Long pauses monotonic timestamps. */
    @GridToStringExclude
    private final long[] longPausesMonotonicTimestamps = new long[EVT_CNT];

    /** Long pauses durations. */
    @GridToStringInclude
    private final long[] longPausesDurations = new long[EVT_CNT];

    /**
     * @param igniteInstanceName Ignite instance name.
     * @param log Logger.
     */
    public LongJVMPauseDetector(String igniteInstanceName, IgniteLogger log) {
        this.igniteInstanceName = igniteInstanceName;
        this.log = log;
    }

    /**
     * Starts worker if not started yet.
     */
    public void start() {
        if (DISABLED) {
            if (log.isDebugEnabled())
                log.debug("JVM Pause Detector is disabled.");

            return;
        }

        final Thread worker = new IgniteThread(igniteInstanceName, "jvm-pause-detector-worker", () -> {
            if (log.isDebugEnabled())
                log.debug(Thread.currentThread().getName() + " has been started.");
            while (true) {
                try {
                    // don't worry, wait will release monitor and all props will be accessible
                    synchronized (this) {
                        lastWakeUpTimeNanos = getMonotonicTimeNanos();
                        long awaitDeadline = lastWakeUpTimeNanos + PRECISION_NANOS;
                        long awaitDeadlineMillis = awaitDeadline / 1_000_000L;
                        int awaitDeadlineNanos = Math.toIntExact(awaitDeadline % 1_000_000);
                        while (getMonotonicTimeNanos() <= awaitDeadline)
                            wait(awaitDeadlineMillis, awaitDeadlineNanos);
                        long nanoTime = getMonotonicTimeNanos();
                        long pause = nanoTime - awaitDeadline;
                        long pauseMillis = TimeUnit.NANOSECONDS.toMillis(pause);
                        if (pauseMillis >= THRESHOLD) {
                            log.warning("Possible too long JVM pause: " + pauseMillis + " ms. " +
                                    "Precision: " + PRECISION + " ms.");
                            final int next = (int)(longPausesCnt++ % EVT_CNT);
                            longPausesTotalDurationNanos += pause;
                            longPausesTimestamps[next] = System.currentTimeMillis();
                            longPausesMonotonicTimestamps[next] = nanoTime;
                            longPausesDurations[next] = pauseMillis;
                        }
                    }
                }
                catch (InterruptedException e) {
                    Thread locThread = Thread.currentThread();

                    if (workerRef.compareAndSet(locThread, null))
                        log.error(locThread.getName() + " has been interrupted.", e);
                    else if (log.isDebugEnabled())
                        log.debug(locThread.getName() + " has been stopped.");

                    break;
                }
            }
        });

        if (!workerRef.compareAndSet(null, worker)) {
            log.warning(LongJVMPauseDetector.class.getSimpleName() + " already started!");

            return;
        }

        worker.setDaemon(true);
        worker.start();

        if (log.isDebugEnabled())
            log.debug("LongJVMPauseDetector was successfully started");
    }

    /**
     * Stops the worker if one is created and running.
     */
    public void stop() {
        final Thread worker = workerRef.getAndSet(null);

        if (worker != null && worker.isAlive() && !worker.isInterrupted())
            worker.interrupt();
    }

    /**
     * @return {@code false} if {@link IgniteSystemProperties#IGNITE_JVM_PAUSE_DETECTOR_DISABLED} set to {@code true},
     * and {@code true} otherwise.
     */
    public static boolean enabled() {
        return !DISABLED;
    }

    /**
     * @return Long JVM pauses count.
     */
    synchronized long longPausesCount() {
        return longPausesCnt;
    }

    /**
     * @return Long JVM pauses total duration.
     */
    synchronized long longPausesTotalDuration() {
        return TimeUnit.NANOSECONDS.toMillis(longPausesTotalDurationNanos);
    }

    /**
     * @return Last long JVM pause events.
     */
    synchronized Map<Long, Long> longPauseEvents() {
        final Map<Long, Long> evts = new TreeMap<>();

        for (int i = 0; i < longPausesTimestamps.length && longPausesTimestamps[i] != 0; i++)
            evts.put(longPausesTimestamps[i], longPausesDurations[i]);

        return evts;
    }

    /**
     * @param cpStart Check point start time in nanos.
     * @return Tries to explain total pauses spotted during check point process
     * or {@link Optional#empty()} if none were found
     */
    public Optional<String> getTotalSpottedPausesExplain(long cpStart) {
        int lastPointer = (int)(longPausesCnt % EVT_CNT);
        int pausesSpottedTimes = 0;
        int curPointer = lastPointer;
        StringBuilder explainBuilder = new StringBuilder();
        synchronized (this) {
            do {
                if (longPausesMonotonicTimestamps[curPointer] <= cpStart)
                    break;
                explainBuilder.append(longPausesDurations[curPointer]).append(" ms at ")
                        .append(longPausesTimestamps[curPointer]).append(";");
                pausesSpottedTimes++;
                curPointer = curPointer == 0 ? EVT_CNT - 1 : curPointer - 1;
            } while (curPointer != lastPointer);
        }
        return pausesSpottedTimes == 0 ?
                Optional.empty() :
                Optional.of(String.format("Pause detecor spotted %d pauses: [%s]. Each with precision %d ms",
                        pausesSpottedTimes,
                        explainBuilder,
                        PRECISION));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(LongJVMPauseDetector.class, this);
    }

    /**
     * @return monotonic time in nanos
     */
    protected static long getMonotonicTimeNanos() {
        return System.nanoTime();
    }
}
