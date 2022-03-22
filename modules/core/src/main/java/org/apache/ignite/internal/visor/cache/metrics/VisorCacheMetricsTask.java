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
import java.util.TreeSet;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.cache.metrics.CacheMetricsSubCommand.ENABLE;

/**
 * Task for a cache metrics command.
 */
@GridInternal
@GridVisorManagementTask
public class VisorCacheMetricsTask extends VisorOneNodeTask<VisorCacheMetricsTaskArg, VisorCacheMetricsTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheMetricsJob job(VisorCacheMetricsTaskArg arg) {
        return new VisorCacheMetricsJob(arg, false);
    }

    /**
     * Job result is a collection of processed cache names.
     */
    private static class VisorCacheMetricsJob extends VisorJob<VisorCacheMetricsTaskArg,
        VisorCacheMetricsTaskResult> {
        /** */
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
                switch (arg.subCommand()) {
                    case ENABLE:
                    case DISABLE:
                        return new VisorCacheMetricsTaskResult(toggleCacheMetricsMode(arg));
                    case STATUS:
                        return new VisorCacheMetricsTaskResult(cacheMetricsStatus(arg));
                }
            }

            return null;
        }

        /**
         * @param arg Task argument.
         */
        private TreeSet<String> toggleCacheMetricsMode(VisorCacheMetricsTaskArg arg) {
            Collection<String> allCacheNames = ignite.cacheNames();
            Collection<String> foundCachesFromArg = F.view(arg.cacheNames(), allCacheNames::contains);

            Collection<String> cacheNames = arg.applyToAllCaches() ? allCacheNames : foundCachesFromArg;

            ignite.cluster().enableStatistics(cacheNames, ENABLE == arg.subCommand());

            return new TreeSet<>(cacheNames);
        }

        /**
         * @param arg Task argument.
         */
        private Map<String, Boolean> cacheMetricsStatus(VisorCacheMetricsTaskArg arg) {
            Collection<String> allCacheNames = ignite.cacheNames();
            Collection<String> foundCachesFromArg = F.view(arg.cacheNames(), allCacheNames::contains);

            Collection<String> cacheNames = arg.applyToAllCaches() ? allCacheNames : foundCachesFromArg;

            Map<String, Boolean> cacheMetricsStatus = new TreeMap<>();

            for (String cacheName : cacheNames) {
                IgniteInternalCache<Object, Object> cacheEx = ignite.cachex(cacheName);

                if (cacheEx != null)
                    cacheMetricsStatus.put(cacheName, cacheEx.clusterMetrics().isStatisticsEnabled());
            }

            return cacheMetricsStatus;
        }
    }
}
