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

package org.apache.ignite.platform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.binary.BinaryArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_USE_BINARY_ARRAYS;

/**
 * Task to set {@link IgniteSystemProperties#IGNITE_USE_BINARY_ARRAYS} to {@code true}.
 */
public class PlatformSetUseBinaryArrayTask extends ComputeTaskAdapter<Boolean, Void> {
    /** {@inheritDoc} */
    @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Boolean useTypedArr) throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        for (ClusterNode node : subgrid)
            jobs.putIfAbsent(new SetUseTypedArrayJob(useTypedArr), node);

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Void reduce(List<ComputeJobResult> results) throws IgniteException {
        return null;
    }

    /** Job. */
    private static class SetUseTypedArrayJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        public SetUseTypedArrayJob(@Nullable Object arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object execute() {
            System.setProperty(IGNITE_USE_BINARY_ARRAYS, Boolean.toString(argument(0)));
            BinaryArray.initUseBinaryArrays();

            return null;
        }
    }
}
