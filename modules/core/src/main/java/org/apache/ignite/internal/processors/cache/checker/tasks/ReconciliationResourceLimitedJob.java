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

package org.apache.ignite.internal.processors.cache.checker.tasks;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.diagnostic.ReconciliationExecutionContext;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.LoggerResource;

/**
 * Abstract class for jobs that are executed as a part of reconciliation workflow with a limited thread-per-node usage.
 */
public abstract class ReconciliationResourceLimitedJob extends ComputeJobAdapter {
    /** Ignite instance. */
    @IgniteInstanceResource
    protected IgniteEx ignite;

    /** Injected logger. */
    @LoggerResource
    protected IgniteLogger log;

    /** Compute job context. */
    @JobContextResource
    protected ComputeJobContext jobCtx;

    /** {@inheritDoc} */
    @Override public Object execute() throws IgniteException {
        ReconciliationExecutionContext execCtx = ignite.context().diagnostic().reconciliationExecutionContext();

        boolean freeThreadsAvailable = execCtx.acquireJobPermitOrHold(sessionId(), jobCtx);

        if (!freeThreadsAvailable)
            return null;

        try {
            return execute0();
        }
        finally {
            execCtx.releaseJobPermit(sessionId());
        }
    }

    /**
     * @return Reconciliation session ID.
     */
    protected abstract long sessionId();

    /**
     * Executes the job logic itself.
     *
     * @return Job result.
     */
    protected abstract Object execute0();
}
