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

package org.apache.ignite.yardstick.compute.model;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Assigns {@link NoopJob} job for each node.
 */
public class NoopTask implements ComputeTask<Object, Object> {
    /** Number of jobs */
    private int jobs;

    /**
     * @param jobs Number of jobs
     */
    public NoopTask(int jobs) {
        assert jobs > 0;

        this.jobs = jobs;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(
        ComputeJobResult res,
        List<ComputeJobResult> rcvd
    ) throws IgniteCheckedException {
        return ComputeJobResultPolicy.WAIT;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        @Nullable Object arg
    ) throws IgniteCheckedException {
        Map<ComputeJob, ClusterNode> map = new HashMap<>((int)(subgrid.size() * jobs / 0.75));

        for (ClusterNode gridNode : subgrid) {
            //assigns jobs for each node
            for (int i = 0; i < jobs; ++i)
                map.put(new NoopJob(), gridNode);
        }

        return map;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
        return null;
    }

    /**
     *
     */
    public static class NoopJob implements ComputeJob, Externalizable {
        /** {@inheritDoc} */
        @Nullable @Override public Object execute() throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            //No-op
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            //No-op
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            //No-op
        }
    }
}
