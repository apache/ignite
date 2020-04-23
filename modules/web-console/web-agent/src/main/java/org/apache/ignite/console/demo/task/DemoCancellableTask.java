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
package org.apache.ignite.console.demo.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Simple compute task to test task cancellation from Visor.
 */
public class DemoCancellableTask implements ComputeTask<Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Void arg) throws IgniteException {
        HashMap<ComputeJob, ClusterNode> map = U.newHashMap(1);

        map.put(new DemoCancellableJob(), subgrid.get(0));

        return map;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        return ComputeJobResultPolicy.WAIT;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
        return null;
    }

    /**
     * Simple compute job to execute cancel action.
     */
    private static class DemoCancellableJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Random generator. */
        private static final Random rnd = new Random();

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            try {
                Thread.sleep(1000 + rnd.nextInt(60000));
            }
            catch (InterruptedException e) {
                // Restore interrupt status
                Thread.currentThread().interrupt();

                throw new IgniteInterruptedException(e);
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DemoCancellableJob.class, this);
        }
    }
}
