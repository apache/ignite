package org.apache.ignite.internal.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

public class TimeBag {
    private final long initializeTime;

    private final List<Stage> stages = new ArrayList<>();

    public TimeBag() {
        this.initializeTime = System.nanoTime();
    }

    private @Nullable Stage lastRecordedStage() {
        if (stages.isEmpty())
            return null;

        return stages.get(stages.size() - 1);
    }

    public synchronized void addStage(String description) {
        long endTime = System.nanoTime();

        Stage lastStage = lastRecordedStage();

        if (lastStage == null)
            stages.add(new Stage(description, initializeTime, endTime));
        else
            stages.add(new Stage(description, lastStage.endTime, endTime));
    }

    public synchronized void addSubstage(String description) {

    }

    public static class Stage {
        private final String description;

        private final long startTime;

        private final long endTime;

        private final Map<Object, List<Stage>> subStages = new HashMap<>();

        public Stage(String description, long startTime, long endTime) {
            this.description = description;
            this.startTime = startTime;
            this.endTime = endTime;
        }
    }
}
