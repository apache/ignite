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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.concurrent.Executor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.jetbrains.annotations.Nullable;

/**
 * Listener which methods will be called in a corresponded checkpoint life cycle period.
 */
public interface CheckpointListener {
    /**
     * Context with information about current snapshots.
     */
    public interface Context {
        /**
         * @return Checkpoint progress callback.
         */
        public CheckpointProgress progress();

        /**
         * @param flush If {@code True} then will flush WAL after a Checkpoint begin.
         */
        public void walFlush(boolean flush);

        /**
         * Whether to flush WAL after a Checkpoint begin.
         */
        public boolean walFlush();

        /**
         * @return Checkpoint future which will be completed when checkpoint ends.
         */
        public IgniteInternalFuture<?> finishedStateFut();

        /**
         * @return Partition allocation statistic map
         */
        public PartitionAllocationMap partitionStatMap();

        /**
         * @return Context executor.
         */
        @Nullable public Executor executor();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException;

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void onCheckpointBegin(Context ctx) throws IgniteCheckedException;

    /**
     * Do some actions before checkpoint write lock.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException;

    /**
     * Do some actions after checkpoint end.
     *
     * @throws IgniteCheckedException If failed.
     */
    default void afterCheckpointEnd(Context ctx) throws IgniteCheckedException {

    }
}
