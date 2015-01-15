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

package org.gridgain.grid.kernal.processors.streamer;

import org.apache.ignite.streamer.*;
import org.jetbrains.annotations.*;

/**
 * Streamer MBean implementation.
 */
public class StreamerMBeanAdapter implements StreamerMBean {
    /** Streamer. */
    private IgniteStreamerImpl streamer;

    /**
     * @param streamer Streamer.
     */
    public StreamerMBeanAdapter(IgniteStreamerImpl streamer) {
        this.streamer = streamer;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String getName() {
        return streamer.name();
    }

    /** {@inheritDoc} */
    @Override public boolean isAtLeastOnce() {
        return streamer.atLeastOnce();
    }

    /** {@inheritDoc} */
    @Override public int getStageFutureMapSize() {
        return streamer.stageFutureMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getBatchFutureMapSize() {
        return streamer.batchFutureMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getStageActiveExecutionCount() {
        return streamer.metrics().stageActiveExecutionCount();
    }

    /** {@inheritDoc} */
    @Override public int getStageWaitingExecutionCount() {
        return streamer.metrics().stageWaitingExecutionCount();
    }

    /** {@inheritDoc} */
    @Override public long getStageTotalExecutionCount() {
        return streamer.metrics().stageTotalExecutionCount();
    }

    /** {@inheritDoc} */
    @Override public long getPipelineMaximumExecutionTime() {
        return streamer.metrics().pipelineMaximumExecutionTime();
    }

    /** {@inheritDoc} */
    @Override public long getPipelineMinimumExecutionTime() {
        return streamer.metrics().pipelineMinimumExecutionTime();
    }

    /** {@inheritDoc} */
    @Override public long getPipelineAverageExecutionTime() {
        return streamer.metrics().pipelineAverageExecutionTime();
    }

    /** {@inheritDoc} */
    @Override public int getPipelineMaximumExecutionNodes() {
        return streamer.metrics().pipelineMaximumExecutionNodes();
    }

    /** {@inheritDoc} */
    @Override public int getPipelineMinimumExecutionNodes() {
        return streamer.metrics().pipelineMinimumExecutionNodes();
    }

    /** {@inheritDoc} */
    @Override public int getPipelineAverageExecutionNodes() {
        return streamer.metrics().pipelineAverageExecutionNodes();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentActiveSessions() {
        return streamer.metrics().currentActiveSessions();
    }

    /** {@inheritDoc} */
    @Override public int getMaximumActiveSessions() {
        return streamer.metrics().maximumActiveSessions();
    }

    /** {@inheritDoc} */
    @Override public int getFailuresCount() {
        return streamer.metrics().failuresCount();
    }
}
