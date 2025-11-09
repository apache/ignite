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
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.NO_PERMISSIONS;

/** Task to enable rolling upgrade. */
@GridInternal
public class RollingUpgradeEnableTask extends VisorOneNodeTask<RollingUpgradeEnableCommandArg, RollingUpgradeTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<RollingUpgradeEnableCommandArg, RollingUpgradeTaskResult> job(RollingUpgradeEnableCommandArg arg) {
        return new RollingUpgradeEnableJob(arg, debug);
    }

    /** */
    private static class RollingUpgradeEnableJob extends VisorJob<RollingUpgradeEnableCommandArg, RollingUpgradeTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        protected RollingUpgradeEnableJob(RollingUpgradeEnableCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override public SecurityPermissionSet requiredPermissions() {
            return NO_PERMISSIONS;
        }

        /** {@inheritDoc} */
        @Override protected RollingUpgradeTaskResult run(RollingUpgradeEnableCommandArg arg) throws IgniteException {
            RollingUpgradeProcessor proc = ignite.context().rollingUpgrade();

            try {
                proc.enable(IgniteProductVersion.fromString(arg.targetVersion()), arg.force());

                IgnitePair<IgniteProductVersion> rollUpVers = proc.versions();

                return new RollingUpgradeTaskResult(
                    rollUpVers == null ? null : rollUpVers.get1(),
                    rollUpVers == null ? null : rollUpVers.get2(),
                    null
                );
            }
            catch (IgniteCheckedException e) {
                IgnitePair<IgniteProductVersion> rollUpVers = proc.versions();

                return new RollingUpgradeTaskResult(
                    rollUpVers == null ? null : rollUpVers.get1(),
                    rollUpVers == null ? null : rollUpVers.get2(),
                    e.getMessage()
                );
            }
        }

    }
}
