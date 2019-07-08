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

package org.apache.ignite.internal.visor.ru;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.ru.RollingUpgradeStatus;
import org.apache.ignite.internal.processors.ru.RollingUpgradeUtil;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Gets cluster-wide state of rolling upgrade.
 */
@GridInternal
@GridVisorManagementTask
public class VisorRollingUpgradeStatusTask extends VisorOneNodeTask<Void, VisorRollingUpgradeStatusResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<Void, VisorRollingUpgradeStatusResult> job(Void arg) {
        return new VisorRollingUpgradeStatusJob(arg, debug);
    }

    /**
     * Job that actually gets the status of rolling upgrade.
     */
    private static class VisorRollingUpgradeStatusJob extends VisorJob<Void, VisorRollingUpgradeStatusResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Creates the job.
         *
         * @param info Snapshot info.
         * @param debug Debug flag.
         */
        VisorRollingUpgradeStatusJob(Void info, boolean debug) {
            super(info, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorRollingUpgradeStatusResult run(Void arg) throws IgniteException {
            RollingUpgradeStatus state = ignite.context().rollingUpgrade().getStatus();

            return new VisorRollingUpgradeStatusResult(
                state,
                RollingUpgradeUtil.initialNodes(ignite.context(), state),
                RollingUpgradeUtil.updatedNodes(ignite.context(), state));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorRollingUpgradeStatusJob.class, this);
        }
    }
}
