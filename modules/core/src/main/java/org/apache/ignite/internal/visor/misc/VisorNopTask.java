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

package org.apache.ignite.internal.visor.misc;

import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Nop task with random timeout.
 */
public class VisorNopTask implements ComputeTask<Integer, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Integer arg) {

        Map<ComputeJob, ClusterNode> map = new GridLeanMap<>(subgrid.size());

        for (ClusterNode node : subgrid)
            map.put(new VisorNopJob(arg), node);

        return map;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res,
        List<ComputeJobResult> rcvd) {
        return ComputeJobResultPolicy.WAIT;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Void reduce(List<ComputeJobResult> results) {
        return null;
    }

    /**
     * Nop job with random timeout.
     */
    private static class VisorNopJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Job argument.
         */
        private VisorNopJob(@Nullable Object arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("ConstantConditions")
        @Nullable @Override public Object execute() {
            try {
                Integer maxTimeout = argument(0);

                Thread.sleep(new Random().nextInt(maxTimeout));
            }
            catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorNopJob.class, this);
        }
    }
}