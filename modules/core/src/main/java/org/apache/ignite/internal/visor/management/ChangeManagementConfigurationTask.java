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

package org.apache.ignite.internal.visor.management;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.management.ManagementConfiguration;
import org.apache.ignite.internal.processors.management.ManagementConsoleProcessorAdapter;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Sets cluster-wide configuration for management.
 */
@GridInternal
@GridVisorManagementTask
public class ChangeManagementConfigurationTask extends VisorOneNodeTask<ManagementConfiguration, ManagementConfiguration> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<ManagementConfiguration, ManagementConfiguration> job(ManagementConfiguration arg) {
        return new ChangeManagementConfigurationTask.ChangeManagementConfigurationJob(arg, debug);
    }

    /**
     * Job that actually gets the status of rolling upgrade.
     */
    private static class ChangeManagementConfigurationJob extends VisorJob<ManagementConfiguration, ManagementConfiguration> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Creates the job.
         *
         * @param arg Snapshot info.
         * @param debug Debug flag.
         */
        ChangeManagementConfigurationJob(ManagementConfiguration arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected ManagementConfiguration run(ManagementConfiguration cfg) throws IgniteException {
            ManagementConsoleProcessorAdapter processor = ignite.context().managementConsole();

            if (cfg != null && !cfg.equals(processor.configuration()))
                processor.configuration(cfg);

            return processor.configuration();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ChangeManagementConfigurationTask.ChangeManagementConfigurationJob.class, this);
        }
    }
}
