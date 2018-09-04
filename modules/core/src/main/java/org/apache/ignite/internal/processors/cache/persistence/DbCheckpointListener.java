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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.concurrent.ExecutorService;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.CheckpointMetricsTracker;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;

/**
 *
 */
public interface DbCheckpointListener {
    /**
     * Context with information about current snapshots.
     */
    public interface Context {
        /**
         *
         */
        public boolean nextSnapshot();

        /**
         * @return Partition allocation statistic map
         */
        public PartitionAllocationMap partitionStatMap();

        /**
         * @param cacheOrGrpName Cache or group name.
         */
        public boolean needToSnapshot(String cacheOrGrpName);

        /**
         * Checkpoint metrics tracker.
         *
         * @return Tracker.
         */
        public CheckpointMetricsTracker tracker();

        /**
         * Checkpoint async runner.
         *
         * @return Service.
         */
        public ExecutorService asyncRunner();

        /**
         *
         * @return
         */
        public int concurrency();
    }

    public class SaveMetadataStat {
        private final String name;
        private int pages;
        private int stripes;
        private long duration;

        public SaveMetadataStat(String name) {
            this.name = name;
        }

        public void setDirtyPages(int pages) {
            this.pages = pages;
        }

        public void setStripes(int stripes) {
            this.stripes = stripes;
        }

        public void setDuration(long duration) {
            this.duration = duration;
        }

        public String getName() {
            return name;
        }

        public int getPages() {
            return pages;
        }

        public int getStripes() {
            return stripes;
        }

        public long getDuration() {
            return duration;
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void onCheckpointBegin(Context ctx) throws IgniteCheckedException;
}
