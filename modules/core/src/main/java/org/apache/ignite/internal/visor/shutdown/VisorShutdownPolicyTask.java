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

package org.apache.ignite.internal.visor.shutdown;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.resources.LoggerResource;

/**
 * Shutdown policy task.
 */
@GridInternal
@GridVisorManagementTask
public class VisorShutdownPolicyTask extends VisorOneNodeTask<VisorShutdownPolicyTaskArg, VisorShutdownPolicyTaskResult> {
    /** Serial version id. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorShutdownPolicyTaskArg, VisorShutdownPolicyTaskResult> job(
        VisorShutdownPolicyTaskArg arg) {
        return new VisorShutdownPolicyJob(arg, debug);
    }

    /**
     * Visor job of shutdown policy task.
     */
    private static class VisorShutdownPolicyJob extends VisorJob<VisorShutdownPolicyTaskArg, VisorShutdownPolicyTaskResult> {
        /** Serial version id. */
        private static final long serialVersionUID = 0L;

        /** Logger. */
        @LoggerResource
        private IgniteLogger log;

        /**
         * Constructor of job.
         *
         * @param arg Argumants.
         * @param debug True if debug mode enable.
         */
        protected VisorShutdownPolicyJob(VisorShutdownPolicyTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorShutdownPolicyTaskResult run(VisorShutdownPolicyTaskArg arg) throws IgniteException {
            VisorShutdownPolicyTaskResult res = new VisorShutdownPolicyTaskResult();

            if (arg.getShutdown() != null)
                ignite.cluster().shutdownPolicy(arg.getShutdown());

            res.setShutdown(ignite.cluster().shutdownPolicy());

            return res;
        }
    }
}
