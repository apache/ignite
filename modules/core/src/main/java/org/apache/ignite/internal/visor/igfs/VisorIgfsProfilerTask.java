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

import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Task that parse hadoop profiler logs.
 */
/**
 * Task that parse hadoop profiler logs.
 */
@GridInternal
@Deprecated
public class VisorIgfsProfilerTask extends VisorOneNodeTask<VisorIgfsProfilerTaskArg, List<VisorIgfsProfilerEntry>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorIgfsProfilerJob job(VisorIgfsProfilerTaskArg arg) {
        return new VisorIgfsProfilerJob(arg, debug);
    }

    /**
     * Job that do actual profiler work.
     */
    private static class VisorIgfsProfilerJob extends VisorJob<VisorIgfsProfilerTaskArg, List<VisorIgfsProfilerEntry>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg IGFS name.
         * @param debug Debug flag.
         */
        private VisorIgfsProfilerJob(VisorIgfsProfilerTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected List<VisorIgfsProfilerEntry> run(VisorIgfsProfilerTaskArg arg) {
            throw new IgniteException("IGFS operations are not supported in current version of Ignite");
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorIgfsProfilerJob.class, this);
        }
    }
}
