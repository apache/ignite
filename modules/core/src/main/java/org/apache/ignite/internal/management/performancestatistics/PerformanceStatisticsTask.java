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

package org.apache.ignite.internal.management.performancestatistics;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.performancestatistics.PerformanceStatisticsCommand.PerformanceStatisticsRotateCommandArg;
import org.apache.ignite.internal.management.performancestatistics.PerformanceStatisticsCommand.PerformanceStatisticsStartCommandArg;
import org.apache.ignite.internal.management.performancestatistics.PerformanceStatisticsCommand.PerformanceStatisticsStatusCommandArg;
import org.apache.ignite.internal.management.performancestatistics.PerformanceStatisticsCommand.PerformanceStatisticsStopCommandArg;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/** Represents visor task to manage performance statistics. */
@GridInternal
@GridVisorManagementTask
public class PerformanceStatisticsTask extends VisorOneNodeTask<IgniteDataTransferObject, String> {
    /** Performance statistics enabled status. */
    public static final String STATUS_ENABLED = "Enabled.";

    /** Performance statistics disabled status. */
    public static final String STATUS_DISABLED = "Disabled.";

    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<IgniteDataTransferObject, String> job(IgniteDataTransferObject arg) {
        return new VisorPerformanceStatisticsJob(arg, false);
    }

    /** */
    private static class VisorPerformanceStatisticsJob extends VisorJob<IgniteDataTransferObject, String> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg   Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorPerformanceStatisticsJob(IgniteDataTransferObject arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(IgniteDataTransferObject arg) throws IgniteException {
            try {
                if (arg instanceof PerformanceStatisticsStartCommandArg) {
                    ignite.context().performanceStatistics().startCollectStatistics();

                    return "Started.";
                }
                else if (arg instanceof PerformanceStatisticsStopCommandArg) {
                    ignite.context().performanceStatistics().stopCollectStatistics();

                    return "Stopped.";
                }
                else if (arg instanceof PerformanceStatisticsRotateCommandArg) {
                    ignite.context().performanceStatistics().rotateCollectStatistics();

                    return "Rotated.";
                }
                else if (arg instanceof PerformanceStatisticsStatusCommandArg)
                    return ignite.context().performanceStatistics().enabled() ? STATUS_ENABLED : STATUS_DISABLED;

                throw new IllegalArgumentException("Unknown operation: " + arg.getClass().getSimpleName());
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }
    }
}
