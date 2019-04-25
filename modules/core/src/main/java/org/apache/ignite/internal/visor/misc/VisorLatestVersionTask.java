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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Task for collecting latest version.
 */
@GridInternal
public class VisorLatestVersionTask extends VisorOneNodeTask<Void, String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorLatestVersionJob job(Void arg) {
        return new VisorLatestVersionJob(arg, debug);
    }

    /**
     * Job for collecting latest version.
     */
    private static class VisorLatestVersionJob extends VisorJob<Void, String> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Formal job argument.
         * @param debug Debug flag.
         */
        private VisorLatestVersionJob(Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(Void arg) {
            return ignite.latestVersion();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorLatestVersionJob.class, this);
        }
    }
}