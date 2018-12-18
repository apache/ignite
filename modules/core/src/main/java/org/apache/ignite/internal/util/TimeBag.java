package org.apache.ignite.internal.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteLogger;

public class TimeBag {
    /** Initial global stage. */
    private static final GlobalStage INITIAL_STAGE = new GlobalStage("", 0, new HashMap<>());

    /** Padding element for pretty print. */
    private static final String PADDING = "  ^-- ";

    /** Lock. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Global stopwatch. */
    private final IgniteStopwatch globalStopwatch = IgniteStopwatch.createStarted();

    /** Logger. */
    private final IgniteLogger log;

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


    public TimeBag(IgniteLogger log) {
        this(log, TimeUnit.MILLISECONDS);
    }

    public TimeBag(IgniteLogger log, TimeUnit measurementUnit) {
        this.log = log;
        this.stages = new ArrayList<>();
        this.localStages = new ConcurrentHashMap<>();
        this.measurementUnit = measurementUnit;

        this.stages.add(INITIAL_STAGE);
    }

    private GlobalStage lastCompletedGlobalStage() {
        assert !stages.isEmpty() : "No stages :(";

        return stages.get(stages.size() - 1);
    }

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

    private void addStageTiming(Stage stage, List<String> result) {
        StringBuilder sb = new StringBuilder();

        if (!(stage instanceof GlobalStage))
            sb.append(PADDING);

        sb.append(stage.name()).append(' ');
        sb.append('[');
        sb.append("desc=").append(stage.description()).append(", ");
        sb.append("time=").append(stage.time()).append(' ').append(measurementUnitShort());
        sb.append(']');

        result.add(sb.toString());
    }

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

    public List<String> stagesTimings() {
        lock.readLock().lock();

        try {
            List<String> result = new ArrayList<>();

            long totalTime = 0;

            // Skip initial stage.
            for (int i = 1; i < stages.size(); i++) {
                GlobalStage globStage = stages.get(i);

                totalTime += globStage.time();

                addStageTiming(globStage, result);

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
                        result.add(PADDING + "Longest execution thread " + longestTimeThread + ":");

                    List<Stage> longestStagesSeq = locStages.get(longestTimeThread);

                    for (Stage stage : longestStagesSeq)
                        addStageTiming(stage, result);
                }
            }

            result.add("Total time of all stages: " + totalTime + " " + measurementUnitShort());

            return result;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    public static class GlobalStage extends Stage {
        private final Map<String, List<Stage>> localStages;

        public GlobalStage(String description, long time, Map<String, List<Stage>> localStages) {
            super(description, time);

            this.localStages = localStages;
        }

        public Map<String, List<Stage>> localStages() {
            return localStages;
        }

        @Override public String name() {
            return "Global stage";
        }
    }

    public static class Stage {
        private final String description;

        private final long time;

        public Stage(String description, long time) {
            this.description = description;
            this.time = time;
        }

        public String description() {
            return description;
        }

        public long time() {
            return time;
        }

        public String name() {
            return "Local stage";
        }
    }
}
