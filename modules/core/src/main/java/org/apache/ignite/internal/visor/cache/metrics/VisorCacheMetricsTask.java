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

package org.apache.ignite.internal.visor.cache.metrics;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.cache.metrics.CacheMetricsOperation.ENABLE;

/**
 * Task for a cache metrics command.
 */
@GridInternal
@GridVisorManagementTask
public class VisorCacheMetricsTask extends VisorOneNodeTask<VisorCacheMetricsTaskArg, VisorCacheMetricsTaskResult> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheMetricsJob job(VisorCacheMetricsTaskArg arg) {
        return new VisorCacheMetricsJob(arg, false);
    }

    /**
     * Job returns {@link Map} with names of processed caches paired with corresponding metrics collection statuses or
     * exception, caught during execution of job.
     * Results are passed into instance of wrapper class {@link VisorCacheMetricsTaskResult}.
     */
    private static class VisorCacheMetricsJob extends VisorJob<VisorCacheMetricsTaskArg, VisorCacheMetricsTaskResult> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg   Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorCacheMetricsJob(@Nullable VisorCacheMetricsTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorCacheMetricsTaskResult run(@Nullable VisorCacheMetricsTaskArg arg)
            throws IgniteException {
            if (arg != null) {
                Collection<String> cacheNames = F.isEmpty(arg.cacheNames()) ? ignite.cacheNames() : arg.cacheNames();

                try {
                    switch (arg.operation()) {
                        case ENABLE:
                        case DISABLE:
                            ignite.cluster().enableStatistics(cacheNames, ENABLE == arg.operation());

                            return new VisorCacheMetricsTaskResult(cacheMetricsStatus(cacheNames));

                        case STATUS:
                            return new VisorCacheMetricsTaskResult(cacheMetricsStatus(cacheNames));

                        default:
                            throw new IllegalStateException("Unexpected value: " + arg.operation());
                    }
                }
                catch (Exception e) {
                    return new VisorCacheMetricsTaskResult(e);
                }
            }

            return null;
        }

        /**
         * @param cacheNames Cache names.
         */
        private Map<String, Boolean> cacheMetricsStatus(Collection<String> cacheNames) {
            Map<String, Boolean> cacheMetricsStatus = new TreeMap<>();

            for (String cacheName : cacheNames) {
                IgniteInternalCache<?, ?> cachex = ignite.cachex(cacheName);

                if (cachex != null)
                    cacheMetricsStatus.put(cacheName, cachex.clusterMetrics().isStatisticsEnabled());
                else
                    throw new IgniteException("Cache does not exist: " + cacheName);
            }

            return cacheMetricsStatus;
        }
    }
}
