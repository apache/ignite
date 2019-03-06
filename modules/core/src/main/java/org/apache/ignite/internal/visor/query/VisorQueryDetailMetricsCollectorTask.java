/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryDetailMetricsAdapter;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryDetailMetricsKey;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isIgfsCache;
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

            IgniteConfiguration cfg = ignite.configuration();

            GridCacheProcessor cacheProc = ignite.context().cache();

            Collection<String> cacheNames = cacheProc.cacheNames();

            Map<GridCacheQueryDetailMetricsKey, GridCacheQueryDetailMetricsAdapter> aggMetrics = new HashMap<>();

            for (String cacheName : cacheNames) {
                if (!isSystemCache(cacheName) && !isIgfsCache(cfg, cacheName)) {
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
