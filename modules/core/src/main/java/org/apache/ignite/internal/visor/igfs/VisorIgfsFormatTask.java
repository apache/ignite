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
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Format IGFS instance.
 */
@GridInternal
@GridVisorManagementTask
@Deprecated
public class VisorIgfsFormatTask extends VisorOneNodeTask<VisorIgfsFormatTaskArg, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorIgfsFormatJob job(VisorIgfsFormatTaskArg arg) {
        return new VisorIgfsFormatJob(arg, debug);
    }

    /**
     * Job that format IGFS.
     */
    private static class VisorIgfsFormatJob extends VisorJob<VisorIgfsFormatTaskArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg IGFS name to format.
         * @param debug Debug flag.
         */
        private VisorIgfsFormatJob(VisorIgfsFormatTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(VisorIgfsFormatTaskArg arg) {
            throw new IgniteException("IGFS operations are not supported in current version of Ignite");
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorIgfsFormatJob.class, this);
        }
    }
}
