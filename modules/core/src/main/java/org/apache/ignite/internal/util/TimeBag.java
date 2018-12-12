package org.apache.ignite.internal.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TimeBag {
    /** Initial global stage. */
    private static final GlobalStage INITIAL_STAGE = new GlobalStage("", 0, new HashMap<>());

    /** Lock. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Global stopwatch. */
    private final IgniteStopwatch globalStopwatch = IgniteStopwatch.createStarted();

    /** Measurement unit. */
    private final TimeUnit measurementUnit;

    /** List of global stages (guarded by {@code lock}). */
    private final List<GlobalStage> stages;

    /** List of current local stages (guarded by {@code lock}). */
    private Map<String, List<LocalStage>> localStages;

    /** Last seen global stage by thread. */
    private final ThreadLocal<GlobalStage> tlLastSeenStage = ThreadLocal.withInitial(() -> INITIAL_STAGE);

    /** Thread-local stopwatch. */
    private final ThreadLocal<IgniteStopwatch> tlStopwatch = ThreadLocal.withInitial(IgniteStopwatch::createUnstarted);


    public TimeBag() {
        this(TimeUnit.MILLISECONDS);
    }

    public TimeBag(TimeUnit measurementUnit) {
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

            LocalStage stage;

            // We see this stage first time, get elapsed time from global stopwatch and start local stopwatch.
            if (lastSeen != lastCompleted) {
                stage = new LocalStage(description, globalStopwatch.elapsed(measurementUnit));

                tlLastSeenStage.set(lastCompleted);

                localStopWatch.start();
            }
            else {
                stage = new LocalStage(description, localStopWatch.elapsed(measurementUnit));

                localStopWatch.reset().start();
            }

            // Associate local stage with current thread name.
            String threadName = Thread.currentThread().getName();

            localStages.computeIfAbsent(threadName, t -> new ArrayList<>()).add(stage);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    public String stagesPrettyPrint() {
        lock.readLock().lock();

        try {
            StringBuilder prettyPrint = new StringBuilder();



            return prettyPrint.toString();
        }
        finally {
            lock.readLock().unlock();
        }
    }

    public static class GlobalStage extends LocalStage {
        private final Map<String, List<LocalStage>> localStages;

        public GlobalStage(String description, long time, Map<String, List<LocalStage>> localStages) {
            super(description, time);

            this.localStages = localStages;
        }

        public Map<String, List<LocalStage>> localStages() {
            return localStages;
        }
    }

    public static class LocalStage {
        private final String description;

        private final long time;

        public LocalStage(String description, long time) {
            this.description = description;
            this.time = time;
        }

        public String description() {
            return description;
        }

        public long time() {
            return time;
        }
    }
}
