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

package org.apache.ignite.internal.visor.misc;

import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Task for changing grid active state.
 */
@GridInternal
@GridVisorManagementTask
public class VisorChangeGridActiveStateTask extends VisorOneNodeTask<VisorChangeGridActiveStateTaskArg, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorChangeGridActiveStateJob job(VisorChangeGridActiveStateTaskArg arg) {
        return new VisorChangeGridActiveStateJob(arg, debug);
    }

    /**
     * Job for changing grid active state.
     */
    private static class VisorChangeGridActiveStateJob extends VisorJob<VisorChangeGridActiveStateTaskArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg New state of grid.
         * @param debug Debug flag.
         */
        private VisorChangeGridActiveStateJob(VisorChangeGridActiveStateTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(VisorChangeGridActiveStateTaskArg arg) {
            ignite.active(arg.isActive());

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorChangeGridActiveStateJob.class, this);
        }
    }
}
