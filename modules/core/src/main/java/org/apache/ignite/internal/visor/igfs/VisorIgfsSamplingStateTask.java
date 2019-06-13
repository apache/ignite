/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Task to set IGFS instance sampling state.
 */
@GridInternal
@Deprecated
public class VisorIgfsSamplingStateTask extends VisorOneNodeTask<VisorIgfsSamplingStateTaskArg, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Job that perform parsing of IGFS profiler logs.
     */
    private static class VisorIgfsSamplingStateJob extends VisorJob<VisorIgfsSamplingStateTaskArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        private VisorIgfsSamplingStateJob(VisorIgfsSamplingStateTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(VisorIgfsSamplingStateTaskArg arg) {
            throw new IgniteException("IGFS operations are not supported in current version of Ignite");
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorIgfsSamplingStateJob.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorIgfsSamplingStateJob job(VisorIgfsSamplingStateTaskArg arg) {
        return new VisorIgfsSamplingStateJob(arg, debug);
    }
}
