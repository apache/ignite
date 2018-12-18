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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class TimeBag {
    /** Initial global stage. */
    private static final GlobalStage INITIAL_STAGE = new GlobalStage("", 0, new HashMap<>());

    /** Padding element for pretty print. */
    private static final String PADDING = "  ^-- ";

    /** Lock. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Global stopwatch. */
    private final IgniteStopwatch globalStopwatch = IgniteStopwatch.createStarted();

    /** Measurement unit. */
    private final TimeUnit measurementUnit;

    /** List of global stages (guarded by {@code lock}). */
    private final List<GlobalStage> stages;

    /** List of current local stages separated by threads (guarded by {@code lock}). */
    private Map<String, List<Stage>> localStages;

    /** Last seen global stage by thread. */
    private final ThreadLocal<GlobalStage> tlLastSeenStage = ThreadLocal.withInitial(() -> INITIAL_STAGE);

    /** Thread-local stopwatch. */
    private final ThreadLocal<IgniteStopwatch> tlStopwatch = ThreadLocal.withInitial(IgniteStopwatch::createUnstarted);


    /**
     * Default constructor.
     */
    public TimeBag() {
        this(TimeUnit.MILLISECONDS);
    }

    /**
     * @param measurementUnit Measurement unit.
     */
    public TimeBag(TimeUnit measurementUnit) {
        this.stages = new ArrayList<>();
        this.localStages = new ConcurrentHashMap<>();
        this.measurementUnit = measurementUnit;

        this.stages.add(INITIAL_STAGE);
    }

    /**
     *
     */
    private GlobalStage lastCompletedGlobalStage() {
        assert !stages.isEmpty() : "No stages :(";

        return stages.get(stages.size() - 1);
    }

    /**
     * @param description Description.
     */
    public void finishGlobalStage(String description) {
        if (!lock.writeLock().tryLock())
            throw new IllegalStateException("Attempt to finish global stage, while local stage finishing is in progress.");

        try {
            stages.add(
                new GlobalStage(description, globalStopwatch.elapsed(measurementUnit), Collections.unmodifiableMap(localStages))
            );

            localStages = new ConcurrentHashMap<>();

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
        if (!lock.readLock().tryLock())
            throw new IllegalStateException("Attempt to finish local stage, while global stage finishing is in progress.");

        try {
            GlobalStage lastSeen = tlLastSeenStage.get();
            GlobalStage lastCompleted = lastCompletedGlobalStage();
            IgniteStopwatch localStopWatch = tlStopwatch.get();

            Stage stage;

            // We see this stage first time, get elapsed time from global stopwatch and start local stopwatch.
            if (lastSeen != lastCompleted) {
                stage = new Stage(description, globalStopwatch.elapsed(measurementUnit));

                tlLastSeenStage.set(lastCompleted);
            }
            else
                stage = new Stage(description, localStopWatch.elapsed(measurementUnit));

            localStopWatch.reset().start();

            // Associate local stage with current thread name.
            String threadName = Thread.currentThread().getName();

            localStages.computeIfAbsent(threadName, t -> new ArrayList<>()).add(stage);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @param stage Stage.
     * @param timingsList List with timings in string representation.
     */
    private void addStageTiming(Stage stage, List<String> timingsList, @Nullable String postfix) {
        StringBuilder sb = new StringBuilder();

        if (!(stage instanceof GlobalStage))
            sb.append(PADDING);

        sb.append(stage.name()).append(' ');
        sb.append('[');
        sb.append("desc=").append(stage.description()).append(", ");
        sb.append("time=").append(stage.time()).append(' ').append(measurementUnitShort());
        if (postfix != null)
            sb.append(", ").append(postfix);
        sb.append(']');

        timingsList.add(sb.toString());
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
    public List<String> stagesTimings(@Nullable String postfix) {
        lock.readLock().lock();

        try {
            List<String> timings = new ArrayList<>();

            long totalTime = 0;

            // Skip initial stage.
            for (int i = 1; i < stages.size(); i++) {
                GlobalStage globStage = stages.get(i);

                totalTime += globStage.time();

                addStageTiming(globStage, timings, postfix);

                if (!globStage.localStages.isEmpty()) {
                    // Take a thread with longest local stages sequence.
                    Map<String, List<Stage>> locStages = globStage.localStages;

                    String longestTimeThread = null;
                    long longestTime = -1;

                    for (Map.Entry<String, List<Stage>> locStage : locStages.entrySet()) {
                        long locStagesSummaryTime = locStage.getValue().stream()
                            .map(stage -> stage.time)
                            .reduce((a, b) -> a + b)
                            .orElse(-1L);

                        if (locStagesSummaryTime > longestTime) {
                            longestTime = locStagesSummaryTime;
                            longestTimeThread = locStage.getKey();
                        }
                    }

                    assert longestTimeThread != null;

                    if (locStages.size() > 1)
                        timings.add(PADDING + "Longest execution thread " + longestTimeThread
                            + " [time=" + longestTime + " " + measurementUnitShort() + "]:");

                    List<Stage> longestStagesSeq = locStages.get(longestTimeThread);

                    for (Stage stage : longestStagesSeq)
                        addStageTiming(stage, timings, postfix);
                }
            }

            // Add last stage with summary time of all global stages.
            addStageTiming(new GlobalStage("Total time", totalTime, Collections.emptyMap()), timings, postfix);

            return timings;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     *
     */
    public static class GlobalStage extends Stage {
        /** Local stages. */
        private final Map<String, List<Stage>> localStages;

        /**
         * @param description Description.
         * @param time Time.
         * @param localStages Local stages.
         */
        public GlobalStage(String description, long time, Map<String, List<Stage>> localStages) {
            super(description, time);

            this.localStages = localStages;
        }

        /**
         *
         */
        public Map<String, List<Stage>> localStages() {
            return localStages;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return "Global stage";
        }
    }

    /**
     *
     */
    public static class Stage {
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

        /**
         *
         */
        public String name() {
            return "Local stage";
        }
    }
}
