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

package org.apache.ignite.internal.visor.client;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.mxbean.ClientProcessorMXBean;
import org.jetbrains.annotations.Nullable;

/**
 * Task to cancel client connection(s).
 */
@GridInternal
@GridVisorManagementTask
public class VisorClientConnectionDropTask extends VisorOneNodeTask<Long, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<Long, Void> job(Long arg) {
        return new VisorClientConnectionDropJob(arg, debug);
    }

    /**
     * Job to cancel client connection(s).
     */
    private static class VisorClientConnectionDropJob extends VisorJob<Long, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorClientConnectionDropJob(@Nullable Long arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(@Nullable Long arg) throws IgniteException {
            ClientProcessorMXBean bean = ignite.context().sqlListener().mxBean();

            if (arg == null)
                bean.dropAllConnections();
            else
                bean.dropConnection(arg);

            return null;
        }
    }
}
