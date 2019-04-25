/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.platform.lifecycle;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Lifecycle task.
 */
public class PlatformJavaLifecycleTask extends ComputeTaskAdapter<Object, List<Integer>> {
    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Object arg) {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        jobs.put(new LifecycleJob(), subgrid.get(0));

        return jobs;
    }

    /** {@inheritDoc} */
    @Nullable @Override public List<Integer> reduce(List<ComputeJobResult> results) {
        return results.get(0).getData();
    }

    /**
     * Job.
     */
    private static class LifecycleJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Nullable @Override public Object execute() {
            List<Integer> res = new ArrayList<Integer>();

            res.add(PlatformJavaLifecycleBean.beforeStartCnt);
            res.add(PlatformJavaLifecycleBean.afterStartCnt);

            return res;
        }
    }
}