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

package org.apache.ignite.internal.visor.igfs;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;


/**
 * Remove all IGFS profiler logs.
 */
@GridInternal
@GridVisorManagementTask
@Deprecated
public class VisorIgfsProfilerClearTask extends VisorOneNodeTask<VisorIgfsProfilerClearTaskArg, VisorIgfsProfilerClearTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorIgfsProfilerClearJob job(VisorIgfsProfilerClearTaskArg arg) {
        return new VisorIgfsProfilerClearJob(arg, debug);
    }

    /**
     * Job to clear profiler logs.
     */
    private static class VisorIgfsProfilerClearJob extends VisorJob<VisorIgfsProfilerClearTaskArg, VisorIgfsProfilerClearTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        private VisorIgfsProfilerClearJob(VisorIgfsProfilerClearTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorIgfsProfilerClearTaskResult run(VisorIgfsProfilerClearTaskArg arg) {
            throw new IgniteException("IGFS operations are not supported in current version of Ignite");
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorIgfsProfilerClearJob.class, this);
        }
    }
}
