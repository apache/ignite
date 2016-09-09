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

package org.apache.ignite.internal.processors.entityframework;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Task to purge old EntityFramework cache entries.
 */
public class PlatformDotNetEntityFrameworkPurgeOldEntriesTask extends ComputeTaskAdapter<Object[], Object> {
    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Object[] arg) throws IgniteException {
        Map<ComputeJob, ClusterNode> res = new HashMap<>(subgrid.size());

        for (ClusterNode nodes : subgrid) {
            ComputeJob job = new PlatformDotNetEntityFrameworkPurgeOldEntriesJob(arg);

            res.put(job, nodes);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
        return null;
    }

    /**
     * Job.
     */
    private static class PlatformDotNetEntityFrameworkPurgeOldEntriesJob extends ComputeJobAdapter {

        /**
         * Ctor.
         *
         * @param args Args.
         */
        private PlatformDotNetEntityFrameworkPurgeOldEntriesJob(Object... args) {
            super(args);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object execute() {
            // All arguments are strings.
            // First arg is cache name.
            // The rest are string EntitySet names (without versions).
            Object[] args = arguments();

            return null;
        }
    }
}
