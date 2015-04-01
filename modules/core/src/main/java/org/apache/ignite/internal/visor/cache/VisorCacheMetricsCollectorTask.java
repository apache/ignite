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

import org.apache.ignite.compute.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.visor.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Task that collect cache metrics from all nodes.
 */
@GridInternal
public class VisorCacheMetricsCollectorTask extends VisorMultiNodeTask<
    VisorCacheMetricsCollectorTask.VisorCacheMetricsCollectorArg, Iterable<VisorCacheAggregatedMetrics>,
    Map<String, VisorCacheMetrics>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheMetricsCollectorJob job(VisorCacheMetricsCollectorArg arg) {
        return new VisorCacheMetricsCollectorJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Iterable<VisorCacheAggregatedMetrics> reduce0(List<ComputeJobResult> results) {
        Map<String, VisorCacheAggregatedMetrics> grpAggrMetrics = U.newHashMap(results.size());

        for (ComputeJobResult res : results) {
            if (res.getException() == null && res.getData() instanceof Map<?, ?>) {
                Map<String, VisorCacheMetrics> cms = res.getData();

                for (Map.Entry<String, VisorCacheMetrics> entry : cms.entrySet()) {
                    VisorCacheAggregatedMetrics am = grpAggrMetrics.get(entry.getKey());

                    if (am == null) {
                        am = new VisorCacheAggregatedMetrics(entry.getKey());

                        grpAggrMetrics.put(entry.getKey(), am);
                    }

                    am.metrics().put(res.getNode().id(), entry.getValue());
                }
            }
        }

        // Create serializable result.
        return new ArrayList<>(grpAggrMetrics.values());
    }

    /**
     * Job that collect cache metrics from node.
     */
    private static class VisorCacheMetricsCollectorJob
        extends VisorJob<VisorCacheMetricsCollectorArg, Map<String, VisorCacheMetrics>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Whether to collect metrics for all caches or for specified cache name only.
         * @param debug Debug flag.
         */
        private VisorCacheMetricsCollectorJob(VisorCacheMetricsCollectorArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Map<String, VisorCacheMetrics> run(final VisorCacheMetricsCollectorArg arg) {
            Collection<? extends GridCache<?, ?>> caches = arg.allCaches
                ? ignite.cachesx(new VisorSystemCachesPredicate(arg.systemCaches))
                : F.asList(ignite.cachex(arg.cacheName));

            if (caches != null) {
                Map<String, VisorCacheMetrics> res = U.newHashMap(caches.size());

                for (GridCache<?, ?> c : caches)
                    res.put(c.name(), VisorCacheMetrics.from(c));

                return res;
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheMetricsCollectorJob.class, this);
        }
    }

    /**
     * Arguments for {@link VisorCacheMetricsCollectorTask}.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorCacheMetricsCollectorArg implements Serializable {
        /** Collect metrics for all caches. */
        private final Boolean allCaches;

        /** Include system cache metrics. */
        private final Boolean systemCaches;

        /** Collect metrics for cache with name only. */
        private final String cacheName;

        /**
         * Create task arguments with given parameters.
         *
         * @param allCaches Collect metrics for all caches.
         * @param systemCaches Include system cache metrics.
         * @param cacheName Collect metrics for cache with name only.
         */
        public VisorCacheMetricsCollectorArg(Boolean allCaches, Boolean systemCaches, String cacheName) {
            this.allCaches = allCaches;
            this.systemCaches = systemCaches;
            this.cacheName = cacheName;
        }

        /** @return Collect metrics for all caches. */
        public Boolean allCaches() {
            return allCaches;
        }

        /** @return Include system cache metrics. */
        public Boolean systemCaches() {
            return systemCaches;
        }

        /** @return Collect metrics for cache with name only. */
        public String cacheName() {
            return cacheName;
        }
    }

    private static class VisorSystemCachesPredicate implements IgnitePredicate<GridCache<?,?>> {
        private final Boolean showSystem;

        VisorSystemCachesPredicate(Boolean showSystem) {
            this.showSystem = showSystem;
        }

        @Override public boolean apply(GridCache<?, ?> cache) {
            return showSystem || !CU.isSystemCache(cache.name());
        }
    }
}
