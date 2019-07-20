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
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * The task that represents enabling/disabling rolling upgrade mode.
 */
@GridInternal
@GridVisorManagementTask
public class VisorRollingUpgradeChangeModeTask extends VisorOneNodeTask<VisorRollingUpgradeChangeModeTaskArg, VisorRollingUpgradeChangeModeResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorRollingUpgradeJob job(VisorRollingUpgradeChangeModeTaskArg arg) {
        return new VisorRollingUpgradeJob(arg, debug);
    }

    /**
     * Job that will collect baseline topology information.
     */
    private static class VisorRollingUpgradeJob extends VisorJob<VisorRollingUpgradeChangeModeTaskArg, VisorRollingUpgradeChangeModeResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Formal job argument.
         * @param debug Debug flag.
         */
        private VisorRollingUpgradeJob(VisorRollingUpgradeChangeModeTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorRollingUpgradeChangeModeResult run(
            VisorRollingUpgradeChangeModeTaskArg arg
        ) throws IgniteException {
            switch (arg.getOperation()) {
                case ENABLE:
                    if (arg.isForcedMode()) {
                        return new VisorRollingUpgradeChangeModeResult(
                            ignite.context().rollingUpgrade().enableForcedMode());
                    }

                    return new VisorRollingUpgradeChangeModeResult(ignite.context().rollingUpgrade().setMode(true));

                case DISABLE:
                    return new VisorRollingUpgradeChangeModeResult(ignite.context().rollingUpgrade().setMode(false));

                default:
                    throw new IgniteException("Unexpected rolling upgrade operation arg=[" + arg + ']');
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorRollingUpgradeJob.class, this);
        }
    }
}
