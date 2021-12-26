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

package org.apache.ignite.internal.visor.cache;

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

/**
 * Task for obtaining a cache metrics collection status.
 */
@GridInternal
@GridVisorManagementTask
public class VisorCacheMetricsStatusTask extends VisorOneNodeTask<VisorCacheMetricsStatusTaskArg,
    Map<String, Boolean>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheMetricsStatusJob job(VisorCacheMetricsStatusTaskArg arg) {
        return new VisorCacheMetricsStatusJob(arg, false);
    }

    /**
     * Job result is a map with metrics statuses for processed caches.
     */
    private static class VisorCacheMetricsStatusJob extends VisorJob<VisorCacheMetricsStatusTaskArg,
        Map<String, Boolean>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg   Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorCacheMetricsStatusJob(@Nullable VisorCacheMetricsStatusTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Map<String, Boolean> run(@Nullable VisorCacheMetricsStatusTaskArg arg)
            throws IgniteException {
            Collection<String> allCacheNames = ignite.cacheNames();
            Collection<String> foundCachesFromArg = F.view(arg.cacheNames(), allCacheNames::contains);

            Collection<String> cacheNames = arg.applyToAllCaches() ? allCacheNames : foundCachesFromArg;

            Map<String, Boolean> cacheMetricsStatusInfo = new TreeMap<>();

            for (String cacheName : cacheNames) {
                IgniteInternalCache<Object, Object> cacheEx = ignite.cachex(cacheName);

                if (cacheEx != null)
                    cacheMetricsStatusInfo.put(cacheName, cacheEx.clusterMetrics().isStatisticsEnabled());
            }

            return cacheMetricsStatusInfo;
        }
    }
}
