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

package org.apache.ignite.internal.management.rollingupgrade;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.rollingupgrade.RollingUpgradeProcessor;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteProductVersion;

/** Task to manage rolling upgrade. */
@GridInternal
public class RollingUpgradeTask extends VisorOneNodeTask<RollingUpgradeCommandArg, RollingUpgradeTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<RollingUpgradeCommandArg, RollingUpgradeTaskResult> job(RollingUpgradeCommandArg arg) {
        return new RollingUpgradeJob(arg, debug);
    }

    /** */
    private static class RollingUpgradeJob extends VisorJob<RollingUpgradeCommandArg, RollingUpgradeTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        protected RollingUpgradeJob(RollingUpgradeCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected RollingUpgradeTaskResult run(RollingUpgradeCommandArg arg) throws IgniteException {
            RollingUpgradeProcessor proc = ignite.context().rollingUpgrade();

            try {
                if (arg instanceof RollingUpgradeEnableCommandArg)
                    return enableRollingUpgrade(proc, (RollingUpgradeEnableCommandArg)arg);
                else if (arg instanceof RollingUpgradeDisableCommandArg)
                    return disableRollingUpgrade(proc);

                return new RollingUpgradeTaskResult(proc.enabled(),
                    proc.versions(),
                    new IllegalArgumentException("Unknown operation: " + arg.getClass().getSimpleName()));
            }
            catch (Throwable e) {
                return new RollingUpgradeTaskResult(proc.enabled(), proc.versions(), e);
            }
        }

        /** */
        private RollingUpgradeTaskResult enableRollingUpgrade(
            RollingUpgradeProcessor proc,
            RollingUpgradeEnableCommandArg arg
        ) throws IgniteCheckedException {
            IgniteProductVersion target = IgniteProductVersion.fromString(arg.targetVersion());

            proc.enable(target);

            return new RollingUpgradeTaskResult(proc.enabled(), proc.versions(), null);
        }

        /** */
        private RollingUpgradeTaskResult disableRollingUpgrade(RollingUpgradeProcessor proc) throws IgniteCheckedException {
            proc.disable();

            return new RollingUpgradeTaskResult(proc.enabled(), proc.versions(), null);
        }
    }
}
