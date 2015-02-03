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

package org.apache.ignite.internal.processors.hadoop.proto;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.hadoop.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.resources.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Hadoop protocol task adapter.
 */
public abstract class GridHadoopProtocolTaskAdapter<R> implements ComputeTask<GridHadoopProtocolTaskArguments, R> {
    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable GridHadoopProtocolTaskArguments arg) {
        return Collections.singletonMap(new Job(arg), subgrid.get(0));
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        return ComputeJobResultPolicy.REDUCE;
    }

    /** {@inheritDoc} */
    @Nullable @Override public R reduce(List<ComputeJobResult> results) {
        if (!F.isEmpty(results)) {
            ComputeJobResult res = results.get(0);

            return res.getData();
        }
        else
            return null;
    }

    /**
     * Job wrapper.
     */
    private class Job implements ComputeJob {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @SuppressWarnings("UnusedDeclaration")
        @IgniteJobContextResource
        private ComputeJobContext jobCtx;

        /** Argument. */
        private final GridHadoopProtocolTaskArguments args;

        /**
         * Constructor.
         *
         * @param args Job argument.
         */
        private Job(GridHadoopProtocolTaskArguments args) {
            this.args = args;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object execute() {
            try {
                return run(jobCtx, ignite.hadoop(), args);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }
    }

    /**
     * Run the task.
     *
     * @param jobCtx Job context.
     * @param hadoop Hadoop facade.
     * @param args Arguments.
     * @return Job result.
     * @throws IgniteCheckedException If failed.
     */
    public abstract R run(ComputeJobContext jobCtx, GridHadoop hadoop, GridHadoopProtocolTaskArguments args)
        throws IgniteCheckedException;
}
