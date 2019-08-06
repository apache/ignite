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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.metric.impl.ObjectGauge;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_DISABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_PRECISION;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.internal.processors.cache.CacheMetricsImpl.CACHE_METRICS;

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

    /** Precision. */
    private static final int PRECISION = getInteger(IGNITE_JVM_PAUSE_DETECTOR_PRECISION, 50);

    /** Threshold. */
    private static final int THRESHOLD =
        getInteger(IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD, DEFAULT_JVM_PAUSE_DETECTOR_THRESHOLD);

    /** Event count. */
    private static final int EVT_CNT = getInteger(IGNITE_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT, 20);

    /** Disabled flag. */
    private static final boolean DISABLED =
        getBoolean(IGNITE_JVM_PAUSE_DETECTOR_DISABLED, false);

    /** Logger. */
    private final IgniteLogger log;

    /** Worker reference. */
    private final AtomicReference<Thread> workerRef = new AtomicReference<>();

    /** Long pause count. */
    private LongAdderMetric longPausesCnt;

    /** Long pause total duration. */
    private LongAdderMetric longPausesTotalDuration;

    /** Long jvm pause last events. */
    private ObjectGauge<Map> longJVMPauseLastEvents;

    /** Last detector's wake up time. */
    private long lastWakeUpTime;

    /** Long pauses timestamps. */
    @GridToStringInclude
    private final long[] longPausesTimestamps = new long[EVT_CNT];

    /** Long pauses durations. */
    @GridToStringInclude
    private final long[] longPausesDurations = new long[EVT_CNT];

    /**
     * @param log Logger.
     */
    public LongJVMPauseDetector(IgniteLogger log) {
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

        final Thread worker = new Thread("jvm-pause-detector-worker") {

            @Override public void run() {
                synchronized (LongJVMPauseDetector.this) {
                    lastWakeUpTime = System.currentTimeMillis();
                }

                if (log.isDebugEnabled())
                    log.debug(getName() + " has been started.");

                while (true) {
                    try {
                        Thread.sleep(PRECISION);

                        final long now = System.currentTimeMillis();
                        final long pause = now - PRECISION - lastWakeUpTime;

                        if (pause >= THRESHOLD) {
                            log.warning("Possible too long JVM pause: " + pause + " milliseconds.");

                            synchronized (LongJVMPauseDetector.this) {
                                final int next = (int)(longPausesCnt.value() % EVT_CNT);

                                longPausesCnt.increment();

                                longPausesTotalDuration.add(pause);

                                longPausesTimestamps[next] = now;

                                longPausesDurations[next] = pause;

                                lastWakeUpTime = now;
                            }
                        }
                        else {
                            synchronized (LongJVMPauseDetector.this) {
                                lastWakeUpTime = now;
                            }
                        }
                    }
                    catch (InterruptedException e) {
                        if (workerRef.compareAndSet(this, null))
                            log.error(getName() + " has been interrupted.", e);
                        else if (log.isDebugEnabled())
                            log.debug(getName() + " has been stopped.");

                        break;
                    }
                }
            }
        };

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
    public long longPausesCount() {
        return longPausesCnt.value();
    }

    /**
     * @return Long JVM pauses total duration.
     */
    public long longPausesTotalDuration() {
        return longPausesTotalDuration.value();
    }

    /**
     * @return Last checker's wake up time.
     */
    public synchronized long getLastWakeUpTime() {
        return lastWakeUpTime;
    }

    /**
     * @return Last long JVM pause events.
     */
    public Map longPauseEvents() {
        return longJVMPauseLastEvents.value();
    }

    /**
     * @return Pair ({@code last long pause event time}, {@code pause time duration}) or {@code null}, if long pause
     * wasn't occurred.
     */
    public synchronized @Nullable IgniteBiTuple<Long, Long> getLastLongPause() {
        int lastPauseIdx = (int)((EVT_CNT + longPausesCnt.value() - 1) % EVT_CNT);

        if (longPausesTimestamps[lastPauseIdx] == 0)
            return null;

        return new IgniteBiTuple<>(longPausesTimestamps[lastPauseIdx], longPausesDurations[lastPauseIdx]);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(LongJVMPauseDetector.class, this);
    }

    /**
     * Initialize LongJVMPauseDetector metrics.
     *
     * @param metric GridMetricManager.
     */
    void initMetrics(GridMetricManager metric) {
        MetricRegistry mReg = metric.registry(CACHE_METRICS);

        longPausesCnt = mReg.longAdderMetric("longPausesCnt", "Long pause count.");
        longPausesTotalDuration = mReg.longAdderMetric("longPausesTotalDuration",
            "Long pause total duration.");
        longJVMPauseLastEvents = new ObjectGauge<>("longJVMPauseLastEvents", "Long JVM pause last events.",
            () -> {
                final Map<Long, Long> evts = new TreeMap<>();

                for (int i = 0; i < longPausesTimestamps.length && longPausesTimestamps[i] != 0; i++)
                    evts.put(longPausesTimestamps[i], longPausesDurations[i]);

                return evts;
            }, Map.class);

        mReg.register(longJVMPauseLastEvents);
    }
}
