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

package org.apache.ignite.tests.p2p.classloadproblem;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.toMap;

/**
 * {@link ComputeTask} implementation that breaks p2p class-loading and causes another class to be loaded
 * using p2p when its job is executed.
 * <p>
 * Used to test the situation when p2p class-loading fails due to a peer failing or leaving.
 */
public class ComputeTaskCausingP2PClassLoadProblem extends ComputeTaskAdapter<Object, Object> {
    /***/
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid, @Nullable Object arg
    ) throws IgniteException {
        return subgrid.stream().collect(toMap(node -> new TestJob(), Function.identity()));
    }

    /** {@inheritDoc} */
    @Override public @Nullable Object reduce(List<ComputeJobResult> results) throws IgniteException {
        return null;
    }

    /***/
    private class TestJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            P2PClassLoadingProblems.triggerP2PClassLoadingProblem(getClass(), ignite);

            return null;
        }
    }
}
