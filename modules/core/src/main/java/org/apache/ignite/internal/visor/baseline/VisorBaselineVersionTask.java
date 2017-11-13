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

package org.apache.ignite.internal.visor.baseline;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

/**
 * Task that will set new baseline.
 */
@GridInternal
public class VisorBaselineVersionTask extends VisorOneNodeTask<String, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorBaselineSetJob job(String arg) {
        return new VisorBaselineSetJob(arg, debug);
    }

    /**
     * Job that will set new baseline.
     */
    private static class VisorBaselineSetJob extends VisorJob<String, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        @IgniteInstanceResource
        private Ignite ig;

        /**
         * @param arg Formal job argument.
         * @param debug Debug flag.
         */
        private VisorBaselineSetJob(String arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(@Nullable String arg) throws IgniteException {
            try {
                long targetVer = Long.parseLong(arg);

                if (targetVer > ig.cluster().topologyVersion())
                    throw new IllegalArgumentException("Topology version is ahead of time: " + targetVer);

                ig.cluster().setBaselineTopology(targetVer);

                return null;
            }
            catch (NumberFormatException ignore) {
                throw new IllegalArgumentException("Not a number: " + arg);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorBaselineSetJob.class, this);
        }
    }
}
