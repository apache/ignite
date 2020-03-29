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

package org.apache.ignite.internal.processors.hadoop.impl;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.hadoop.HadoopHelper;
import org.apache.ignite.hadoop.HadoopInputSplit;
import org.apache.ignite.internal.processors.hadoop.HadoopJobEx;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopJobInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.UUID;

/**
 * Mock job for planner tests.
 */
public class HadoopPlannerMockJob extends HadoopJobEx {
    /** Input splits. */
    private final Collection<HadoopInputSplit> splits;

    /** Reducers count. */
    private final int reducers;

    /**
     * Constructor.
     *
     * @param splits Input splits.
     * @param reducers Reducers.
     */
    public HadoopPlannerMockJob(Collection<HadoopInputSplit> splits, int reducers) {
        this.splits = splits;
        this.reducers = reducers;
    }

    /** {@inheritDoc} */
    @Override public Collection<HadoopInputSplit> input() {
        return splits;
    }

    /** {@inheritDoc} */
    @Override public HadoopJobInfo info() {
        return new JobInfo(reducers);
    }

    /** {@inheritDoc} */
    @Override public HadoopJobId id() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public HadoopTaskContext getTaskContext(HadoopTaskInfo info) throws IgniteCheckedException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public void initialize(boolean external, UUID nodeId) throws IgniteCheckedException {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public void dispose(boolean external) throws IgniteCheckedException {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public void prepareTaskEnvironment(HadoopTaskInfo info) throws IgniteCheckedException {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public void cleanupTaskEnvironment(HadoopTaskInfo info) throws IgniteCheckedException {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public void cleanupStagingDirectory() {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public String igniteWorkDirectory() {
        throwUnsupported();

        return null;
    }

    /**
     * Throw {@link UnsupportedOperationException}.
     */
    private static void throwUnsupported() {
        throw new UnsupportedOperationException("Should not be called!");
    }

    /**
     * Mocked job info.
     */
    private static class JobInfo implements HadoopJobInfo {
        /** Reducers. */
        private final int reducers;

        /**
         * Constructor.
         *
         * @param reducers Reducers.
         */
        public JobInfo(int reducers) {
            this.reducers = reducers;
        }

        /** {@inheritDoc} */
        @Override public int reducers() {
            return reducers;
        }

        /** {@inheritDoc} */
        @Nullable @Override public String property(String name) {
            throwUnsupported();

            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean hasCombiner() {
            throwUnsupported();

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean hasReducer() {
            throwUnsupported();

            return false;
        }

        /** {@inheritDoc} */
        @Override public HadoopJobEx createJob(Class<? extends HadoopJobEx> jobCls, HadoopJobId jobId, IgniteLogger log,
            @Nullable String[] libNames, HadoopHelper helper) throws IgniteCheckedException {
            throwUnsupported();

            return null;
        }

        /** {@inheritDoc} */
        @Override public String jobName() {
            throwUnsupported();

            return null;
        }

        /** {@inheritDoc} */
        @Override public String user() {
            throwUnsupported();

            return null;
        }

        @Override public byte[] credentials() {
            throwUnsupported();

            return null;
        }
    }
}
