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

package org.apache.ignite.internal.client.impl.id_and_tag;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
@GridInternal
@GridVisorManagementTask
public class IdAndTagViewTask extends VisorOneNodeTask<Void, IdAndTagViewTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<Void, IdAndTagViewTaskResult> job(Void arg) {
        return new IdAndTagViewJob(arg, debug);
    }

    private static class IdAndTagViewJob extends VisorJob<Void, IdAndTagViewTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        IdAndTagViewJob(Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected IdAndTagViewTaskResult run(@Nullable Void arg) throws IgniteException {
            return view();
        }

        /** */
        private IdAndTagViewTaskResult view() {
            IgniteClusterEx cl = ignite.cluster();

            return new IdAndTagViewTaskResult(cl.id(), cl.tag());
        }
    }
}
