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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;

/**
 * Defragmentation operation service.
 */
public interface IgniteDefragmentation {
    /**
     * Schedule defragmentaton on next start of the node.
     *
     * @param cacheNames Names of caches to run defragmentation on.
     * @return Result of the scheduling.
     * @throws IgniteCheckedException If failed.
     */
    ScheduleResult schedule(List<String> cacheNames) throws IgniteCheckedException;

    /**
     * Cancel scheduled or ongoing defragmentation.
     * @return Result of the cancellation.
     * @throws IgniteCheckedException If failed.
     */
    CancelResult cancel() throws IgniteCheckedException;

    /**
     * Get the status of the ongoing defragmentation.
     * @return Defragmentation status.
     * @throws IgniteCheckedException If failed.
     */
    DefragmentationStatus status() throws IgniteCheckedException;

    /**
     * @return {@code true} if there is an ongoing defragmentation.
     */
    boolean inProgress();

    /**
     * @return Number of processed partitions, or 0 if there is no ongoing defragmentation.
     */
    int processedPartitions();

    /**
     * @return Number of total partitions, or 0 if there is no ongoing defragmentation.
     */
    int totalPartitions();

    /**
     * @return Timestamp of the beginning of the ongoing defragmentation or 0 if there is none.
     */
    long startTime();

    /** Result of the scheduling. */
    public enum ScheduleResult {
        /**
         * Successfully scheduled.
         */
        SUCCESS,

        /**
         * Successfuly scheduled, superseding previously scheduled defragmentation.
         */
        SUCCESS_SUPERSEDED_PREVIOUS
    }

    /** Result of the cancellation. */
    public enum CancelResult {
        /**
         * Cancelled scheduled defragmentation.
         */
        CANCELLED_SCHEDULED,

        /**
         * Nothing to cancel, no ongoing defragmentation.
         */
        SCHEDULED_NOT_FOUND,

        /**
         * Cancelled ongoing defragmentation.
         */
        CANCELLED,

        /**
         * Defragmentation is already completed or cancelled.
         */
        COMPLETED_OR_CANCELLED
    }

    /** */
    public static class DefragmentationStatus {
        /** */
        private final Map<String, CompletedDefragmentationInfo> completedCaches;

        /** */
        private final Map<String, InProgressDefragmentationInfo> inProgressCaches;

        /** */
        private final Set<String> awaitingCaches;

        /** */
        private final Set<String> skippedCaches;

        /** */
        private final int totalPartitions;

        /** */
        private final int processedPartitions;

        /** */
        private final long startTs;

        /** */
        private final long totalElapsedTime;

        public DefragmentationStatus(
                Map<String, CompletedDefragmentationInfo> completedCaches,
                Map<String, InProgressDefragmentationInfo> inProgressCaches,
                Set<String> awaitingCaches,
                Set<String> skippedCaches,
                int totalPartitions,
                int processedPartitions,
                long startTs,
                long totalElapsedTime
        ) {
            this.completedCaches = completedCaches;
            this.inProgressCaches = inProgressCaches;
            this.awaitingCaches = awaitingCaches;
            this.skippedCaches = skippedCaches;
            this.totalPartitions = totalPartitions;
            this.processedPartitions = processedPartitions;
            this.startTs = startTs;
            this.totalElapsedTime = totalElapsedTime;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            StringBuilder sb = new StringBuilder();

            if (!completedCaches.isEmpty()) {
                sb.append("Defragmentation is completed for cache groups:\n");

                for (Map.Entry<String, CompletedDefragmentationInfo> entry : completedCaches.entrySet()) {
                    sb.append("    ").append(entry.getKey()).append(" - ");

                    sb.append(entry.getValue().toString()).append('\n');
                }
            }

            if (!inProgressCaches.isEmpty()) {
                sb.append("Defragmentation is in progress for cache groups:\n");

                for (Map.Entry<String, InProgressDefragmentationInfo> entry : inProgressCaches.entrySet()) {
                    sb.append("    ").append(entry.getKey()).append(" - ");

                    sb.append(entry.getValue().toString()).append('\n');
                }
            }

            if (!skippedCaches.isEmpty())
                sb.append("Skipped cache groups: ").append(String.join(", ", skippedCaches)).append('\n');

            if (!awaitingCaches.isEmpty())
                sb.append("Awaiting defragmentation: ").append(String.join(", ", awaitingCaches)).append('\n');

            return sb.toString();
        }

        /** */
        public Map<String, CompletedDefragmentationInfo> getCompletedCaches() {
            return completedCaches;
        }

        /** */
        public Map<String, InProgressDefragmentationInfo> getInProgressCaches() {
            return inProgressCaches;
        }

        /** */
        public Set<String> getAwaitingCaches() {
            return awaitingCaches;
        }

        /** */
        public Set<String> getSkippedCaches() {
            return skippedCaches;
        }

        /** */
        public long getTotalElapsedTime() {
            return totalElapsedTime;
        }

        /** */
        public int getTotalPartitions() {
            return totalPartitions;
        }

        /** */
        public int getProcessedPartitions() {
            return processedPartitions;
        }

        /** */
        public long getStartTs() {
            return startTs;
        }
    }

    /** */
    abstract class DefragmentationInfo {
        /** */
        long elapsedTime;

        public DefragmentationInfo(long elapsedTime) {
            this.elapsedTime = elapsedTime;
        }

        /** */
        void appendDuration(StringBuilder sb, long elapsedTime) {
            long duration = Math.round(elapsedTime * 1e-3);

            long mins = duration / 60;
            long secs = duration % 60;

            sb.append(mins).append(" mins ").append(secs).append(" secs");
        }

        /** */
        public long getElapsedTime() {
            return elapsedTime;
        }
    }

    /** */
    public static class CompletedDefragmentationInfo extends DefragmentationInfo {
        /** */
        private static final DecimalFormat MB_FORMAT = new DecimalFormat(
            "#.##",
            DecimalFormatSymbols.getInstance(Locale.US)
        );

        /** */
        long sizeBefore;

        /** */
        long sizeAfter;

        public CompletedDefragmentationInfo(long elapsedTime, long sizeBefore, long sizeAfter) {
            super(elapsedTime);
            this.sizeBefore = sizeBefore;
            this.sizeAfter = sizeAfter;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            StringBuilder sb = new StringBuilder();

            double mb = 1024 * 1024;

            sb.append("size before/after: ").append(MB_FORMAT.format(sizeBefore / mb)).append("MB/");
            sb.append(MB_FORMAT.format(sizeAfter / mb)).append("MB");

            sb.append(", time took: ");

            appendDuration(sb, elapsedTime);

            return sb.toString();
        }

        /** */
        public long getSizeBefore() {
            return sizeBefore;
        }

        /** */
        public long getSizeAfter() {
            return sizeAfter;
        }
    }

    /** */
    public static class InProgressDefragmentationInfo extends DefragmentationInfo {
        /** */
        int processedPartitions;

        /** */
        int totalPartitions;

        public InProgressDefragmentationInfo(long elapsedTime, int processedPartitions, int totalPartitions) {
            super(elapsedTime);
            this.processedPartitions = processedPartitions;
            this.totalPartitions = totalPartitions;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append("partitions processed/all: ").append(processedPartitions).append("/").append(totalPartitions);

            sb.append(", time elapsed: ");

            appendDuration(sb, elapsedTime);

            return sb.toString();
        }

        /** */
        public int getProcessedPartitions() {
            return processedPartitions;
        }

        /** */
        public int getTotalPartitions() {
            return totalPartitions;
        }
    }

}
