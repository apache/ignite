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

package org.apache.ignite.internal.visor.compute;

import java.util.List;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.ComputeMXBeanImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Cancels given tasks sessions on all cluster nodes.
 */
@GridInternal
@GridVisorManagementTask
public class VisorComputeCancelSessionTask extends VisorOneNodeTask<VisorComputeCancelSessionTaskArg, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorComputeCancelSessionJob job(VisorComputeCancelSessionTaskArg arg) {
        return new VisorComputeCancelSessionJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Void reduce0(List<ComputeJobResult> results) {
        // No-op, just awaiting all jobs done.
        return null;
    }

    /**
     * Job that cancel tasks.
     */
    private static class VisorComputeCancelSessionJob extends VisorJob<VisorComputeCancelSessionTaskArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Map with task sessions IDs to cancel.
         * @param debug Debug flag.
         */
        private VisorComputeCancelSessionJob(VisorComputeCancelSessionTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(VisorComputeCancelSessionTaskArg arg) {
            new ComputeMXBeanImpl(ignite.context()).cancel(arg.getSessionId());

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorComputeCancelSessionJob.class, this);
        }
    }
}
