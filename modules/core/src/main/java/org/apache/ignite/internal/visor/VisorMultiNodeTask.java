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

package org.apache.ignite.internal.visor;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.resources.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

/**
 * Base class for Visor tasks intended to query data from a multiple node.
 *
 * @param <A> Task argument type.
 * @param <R> Task result type.
 * @param <J> Job result type
 */
public abstract class VisorMultiNodeTask<A, R, J> implements ComputeTask<VisorTaskArgument<A>, R> {
    /** Auto-injected grid instance. */
    @IgniteInstanceResource
    protected transient IgniteEx ignite;

    /** Debug flag. */
    protected boolean debug;

    /** Task argument. */
    protected A taskArg;

    /** Task start time. */
    protected long start;

    /**
     * @param arg Task arg.
     * @return New job.
     */
    protected abstract VisorJob<A, J> job(A arg);

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, VisorTaskArgument<A> arg) {
        assert arg != null;

        start = U.currentTimeMillis();

        debug = arg.debug();

        taskArg = arg.argument();

        if (debug)
            logStart(ignite.log(), getClass(), start);

        return map0(subgrid, arg);
    }

    /**
     * Actual map logic.
     *
     * @param arg Task execution argument.
     * @param subgrid Nodes available for this task execution.
     * @return Map of grid jobs assigned to subgrid node.
     * @throws IgniteException If mapping could not complete successfully.
     */
    protected Map<? extends ComputeJob, ClusterNode> map0(List<ClusterNode> subgrid, VisorTaskArgument<A> arg) {
        Collection<UUID> nodeIds = arg.nodes();

        Map<ComputeJob, ClusterNode> map = U.newHashMap(nodeIds.size());

        try {
            for (ClusterNode node : subgrid)
                if (nodeIds.contains(node.id()))
                    map.put(job(taskArg), node);

            return map;
        }
        finally {
            if (debug)
                logMapped(ignite.log(), getClass(), map.values());
        }
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        // All Visor tasks should handle exceptions in reduce method.
        return ComputeJobResultPolicy.WAIT;
    }

    /**
     * Actual reduce logic.
     *
     * @param results Job results.
     * @return Task result.
     * @throws IgniteException If reduction or results caused an error.
     */
    @Nullable protected abstract R reduce0(List<ComputeJobResult> results) throws IgniteException;

    /** {@inheritDoc} */
    @Nullable @Override public final R reduce(List<ComputeJobResult> results) {
        try {
            return reduce0(results);
        }
        finally {
            if (debug)
                logFinish(ignite.log(), getClass(), start);
        }
    }
}
