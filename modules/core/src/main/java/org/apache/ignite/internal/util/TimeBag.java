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

package org.apache.ignite.internal.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.jetbrains.annotations.NotNull;

/**
 * Utility class to measure and collect timings of some execution workflow.
 */
public class TimeBag {
    /** Initial global stage. */
    private final CompositeStage initStage;

    /** Lock. */
    private final ReentrantReadWriteLock lock;

    /** Global stopwatch. */
    private final IgniteStopwatch globalStopwatch;

    /** Measurement unit. */
    private final TimeUnit measurementUnit;

    /** List of global stages (guarded by {@code lock}). */
    private final List<CompositeStage> stages;

    /** List of current local stages separated by threads (guarded by {@code lock}). */
    private Map<String, List<Stage>> locStages;

    /** Last seen global stage by thread. */
    private final ThreadLocal<CompositeStage> tlLastSeenStage;

    /** Thread-local stopwatch. */
    private final ThreadLocal<IgniteStopwatch> tlStopwatch;

    /** Record flag. */
    private final boolean record;

    /**
     * Default constructor.
     */
    public TimeBag(boolean record) {
        this(TimeUnit.MILLISECONDS, record);
    }

    /**
     * @param measurementUnit Measurement unit.
     */
    public TimeBag(TimeUnit measurementUnit, boolean record) {
        if (record) {
            initStage = new CompositeStage("", 0, new HashMap<>());
            lock = new ReentrantReadWriteLock();
            tlLastSeenStage = ThreadLocal.withInitial(() -> initStage);
            globalStopwatch = IgniteStopwatch.createStarted();
            tlStopwatch = ThreadLocal.withInitial(IgniteStopwatch::createUnstarted);
            stages = new ArrayList<>();
            locStages = new ConcurrentHashMap<>();

            stages.add(initStage);
        }
        else {
            initStage = null;
            lock = null;
            tlLastSeenStage = null;
            globalStopwatch = null;
            tlStopwatch = null;
            stages = null;
            locStages = null;
        }

        this.measurementUnit = measurementUnit;
        this.record = record;
    }

    /**
     *
     */
    private CompositeStage lastCompletedGlobalStage() {
        assert !stages.isEmpty() : "No stages :(";

        return stages.get(stages.size() - 1);
    }

    /**
     * @param description Description.
     */
    public void finishGlobalStage(String description) {
        if (!record)
            return;

        lock.writeLock().lock();

        try {
            stages.add(
                new CompositeStage(description, globalStopwatch.elapsed(measurementUnit), Collections.unmodifiableMap(locStages))
            );

            locStages = new ConcurrentHashMap<>();

            globalStopwatch.reset().start();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param description Description.
     */
    public void finishLocalStage(String description) {
        if (!record)
            return;

        lock.readLock().lock();

        try {
            CompositeStage lastSeen = tlLastSeenStage.get();
            CompositeStage lastCompleted = lastCompletedGlobalStage();
            IgniteStopwatch localStopWatch = tlStopwatch.get();

            Stage stage;

            // We see this stage first time, get elapsed time from last completed global stage and start tracking local.
            if (lastSeen != lastCompleted) {
                stage = new Stage(description, globalStopwatch.elapsed(measurementUnit));

                tlLastSeenStage.set(lastCompleted);
            }
            else
                stage = new Stage(description, localStopWatch.elapsed(measurementUnit));

            localStopWatch.reset().start();

            // Associate local stage with current thread name.
            String threadName = Thread.currentThread().getName();

            locStages.computeIfAbsent(threadName, t -> new ArrayList<>()).add(stage);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @return Short name of desired measurement unit.
     */
    private String measurementUnitShort() {
        switch (measurementUnit) {
            case MILLISECONDS:
                return "ms";
            case SECONDS:
                return "s";
            case NANOSECONDS:
                return "ns";
            case MICROSECONDS:
                return "mcs";
            case HOURS:
                return "h";
            case MINUTES:
                return "min";
            case DAYS:
                return "days";
            default:
                return "";
        }
    }

    /**
     * @return List of string representation of all stage timings.
     */
    public List<String> stagesTimings() {
        assert record;

        lock.readLock().lock();

        try {
            List<String> timings = new ArrayList<>();

            long totalTime = 0;

            // Skip initial stage.
            for (int i = 1; i < stages.size(); i++) {
                CompositeStage stage = stages.get(i);

                totalTime += stage.time();

                timings.add(stage.toString());
            }

            // Add last stage with summary time of all global stages.
            timings.add(new Stage("Total time", totalTime).toString());

            return timings;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @param maxPerCompositeStage Max count of local stages to collect per composite stage.
     * @return List of string represenation of longest local stages per each composite stage.
     */
    public List<String> longestLocalStagesTimings(int maxPerCompositeStage) {
        assert record;

        lock.readLock().lock();

        try {
            List<String> timings = new ArrayList<>();

            for (int i = 1; i < stages.size(); i++) {
                CompositeStage stage = stages.get(i);

                if (!stage.localStages.isEmpty()) {
                    PriorityQueue<Stage> stagesByTime = new PriorityQueue<>();

                    for (Map.Entry<String, List<Stage>> threadAndStages : stage.localStages.entrySet()) {
                        for (Stage locStage : threadAndStages.getValue())
                            stagesByTime.add(locStage);
                    }

                    int stageCount = 0;
                    while (!stagesByTime.isEmpty() && stageCount < maxPerCompositeStage) {
                        stageCount++;

                        Stage locStage = stagesByTime.poll();

                        timings.add(locStage.toString() + " (parent=" + stage.description() + ")");
                    }
                }
            }

            return timings;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     *
     */
    private class CompositeStage extends Stage {
        /** Local stages. */
        private final Map<String, List<Stage>> localStages;

        /**
         * @param description Description.
         * @param time Time.
         * @param localStages Local stages.
         */
        public CompositeStage(String description, long time, Map<String, List<Stage>> localStages) {
            super(description, time);

            this.localStages = localStages;
        }

        /**
         *
         */
        public Map<String, List<Stage>> localStages() {
            return localStages;
        }
    }

    /**
     *
     */
    private class Stage implements Comparable<Stage> {
        /** Description. */
        private final String description;

        /** Time. */
        private final long time;

        /**
         * @param description Description.
         * @param time Time.
         */
        public Stage(String description, long time) {
            this.description = description;
            this.time = time;
        }

        /**
         *
         */
        public String description() {
            return description;
        }

        /**
         *
         */
        public long time() {
            return time;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append("stage=").append('"').append(description()).append('"');
            sb.append(' ').append('(').append(time()).append(' ').append(measurementUnitShort()).append(')');

            return sb.toString();
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull TimeBag.Stage o) {
            if (o.time < time)
                return -1;
            if (o.time > time)
                return 1;
            return o.description.compareTo(description);
        }
    }
}
