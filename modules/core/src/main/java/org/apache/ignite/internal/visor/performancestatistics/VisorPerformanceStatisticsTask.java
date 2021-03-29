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

package org.apache.ignite.internal.visor.performancestatistics;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/** Represents visor task to manage performance statistics. */
@GridInternal
@GridVisorManagementTask
public class VisorPerformanceStatisticsTask extends VisorOneNodeTask<VisorPerformanceStatisticsTaskArg, String> {
    /** Performance statistics enabled status. */
    public static final String STATUS_ENABLED = "Enabled.";

    /** Performance statistics disabled status. */
    public static final String STATUS_DISABLED = "Disabled.";

    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorPerformanceStatisticsTaskArg, String> job(VisorPerformanceStatisticsTaskArg arg) {
        return new VisorPerformanceStatisticsJob(arg, false);
    }

    /** */
    private static class VisorPerformanceStatisticsJob extends VisorJob<VisorPerformanceStatisticsTaskArg, String> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg   Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorPerformanceStatisticsJob(VisorPerformanceStatisticsTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(VisorPerformanceStatisticsTaskArg arg) throws IgniteException {
            try {
                switch (arg.operation()) {
                    case START:
                        ignite.context().performanceStatistics().startCollectStatistics();

                        return "Started.";

                    case STOP:
                        ignite.context().performanceStatistics().stopCollectStatistics();

                        return "Stopped.";

                    case ROTATE:
                        ignite.context().performanceStatistics().rotateCollectStatistics();

                        return "Rotated.";

                    case STATUS:

                        return ignite.context().performanceStatistics().enabled() ? STATUS_ENABLED : STATUS_DISABLED;

                    default:
                        throw new IllegalArgumentException("Unknown operation: " + arg.operation());
                }
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }
    }
}
