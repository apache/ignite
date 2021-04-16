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

package org.apache.ignite.internal.visor.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.QueryDetailMetrics;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryDetailMetricsAdapter;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryDetailMetricsKey;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isSystemCache;

/**
 * Task to collect cache query metrics.
 */
@GridInternal
public class VisorQueryDetailMetricsCollectorTask extends VisorMultiNodeTask<VisorQueryDetailMetricsCollectorTaskArg,
    Collection<VisorQueryDetailMetrics>, Collection<? extends QueryDetailMetrics>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheQueryDetailMetricsCollectorJob job(VisorQueryDetailMetricsCollectorTaskArg arg) {
        return new VisorCacheQueryDetailMetricsCollectorJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Collection<VisorQueryDetailMetrics> reduce0(List<ComputeJobResult> results)
        throws IgniteException {
        Map<GridCacheQueryDetailMetricsKey, GridCacheQueryDetailMetricsAdapter> taskRes = new HashMap<>();

        for (ComputeJobResult res : results) {
            if (res.getException() != null)
                throw res.getException();

            Collection<GridCacheQueryDetailMetricsAdapter> metrics = res.getData();

            VisorCacheQueryDetailMetricsCollectorJob.aggregateMetrics(-1, taskRes, metrics);
        }

        Collection<GridCacheQueryDetailMetricsAdapter> aggMetrics = taskRes.values();

        Collection<VisorQueryDetailMetrics> res = new ArrayList<>(aggMetrics.size());

        for (GridCacheQueryDetailMetricsAdapter m: aggMetrics)
            res.add(new VisorQueryDetailMetrics(m));

        return res;
    }

    /**
     * Job that will actually collect query metrics.
     */
    private static class VisorCacheQueryDetailMetricsCollectorJob
        extends VisorJob<VisorQueryDetailMetricsCollectorTaskArg, Collection<? extends QueryDetailMetrics>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Last time when metrics were collected.
         * @param debug Debug flag.
         */
        protected VisorCacheQueryDetailMetricsCollectorJob(@Nullable VisorQueryDetailMetricsCollectorTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /**
         * @param since Time when metrics were collected last time.
         * @param res Response.
         * @param metrics Metrics.
         */
        private static void aggregateMetrics(long since, Map<GridCacheQueryDetailMetricsKey,
            GridCacheQueryDetailMetricsAdapter> res, Collection<GridCacheQueryDetailMetricsAdapter> metrics) {
            for (GridCacheQueryDetailMetricsAdapter m : metrics) {
                if (m.lastStartTime() > since) {
                    GridCacheQueryDetailMetricsKey key = m.key();

                    GridCacheQueryDetailMetricsAdapter aggMetrics = res.get(key);

                    res.put(key, aggMetrics == null ? m : aggMetrics.aggregate(m));
                }
            }
        }

        /** {@inheritDoc} */
        @Override protected Collection<? extends QueryDetailMetrics> run(
            @Nullable VisorQueryDetailMetricsCollectorTaskArg arg
        ) throws IgniteException {
            assert arg != null;

            GridCacheProcessor cacheProc = ignite.context().cache();

            Collection<String> cacheNames = cacheProc.cacheNames();

            Map<GridCacheQueryDetailMetricsKey, GridCacheQueryDetailMetricsAdapter> aggMetrics = new HashMap<>();

            for (String cacheName : cacheNames) {
                if (!isSystemCache(cacheName)) {
                    IgniteInternalCache<Object, Object> cache = cacheProc.cache(cacheName);

                    if (cache == null || !cache.context().started())
                        continue;

                    aggregateMetrics(arg.getSince(), aggMetrics, cache.context().queries().detailMetrics());
                }
            }

            return new ArrayList<>(aggMetrics.values());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheQueryDetailMetricsCollectorJob.class, this);
        }
    }
}
