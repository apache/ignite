/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.QueryHistoryMetrics;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isSystemCache;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL_FIELDS;

/**
 * Task to collect cache query metrics.
 */
@GridInternal
public class VisorQueryHistoryMetricsCollectorTask extends VisorMultiNodeTask<VisorQueryDetailMetricsCollectorTaskArg,
    Collection<VisorQueryDetailMetrics>, Collection<? extends QueryDetailMetrics>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorQueryHistoryMetricsCollectorJob job(VisorQueryDetailMetricsCollectorTaskArg arg) {
        return new VisorQueryHistoryMetricsCollectorJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Collection<VisorQueryDetailMetrics> reduce0(List<ComputeJobResult> results)
        throws IgniteException {
        Map<GridCacheQueryDetailMetricsKey, GridCacheQueryDetailMetricsAdapter> taskRes = new HashMap<>();

        for (ComputeJobResult res : results) {
            if (res.getException() != null)
                throw res.getException();

            Collection<GridCacheQueryDetailMetricsAdapter> metrics = res.getData();

            VisorQueryHistoryMetricsCollectorJob.aggregateMetrics(-1, taskRes, metrics, false);
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
    private static class VisorQueryHistoryMetricsCollectorJob
        extends VisorJob<VisorQueryDetailMetricsCollectorTaskArg, Collection<? extends QueryDetailMetrics>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Last time when metrics were collected.
         * @param debug Debug flag.
         */
        protected VisorQueryHistoryMetricsCollectorJob(@Nullable VisorQueryDetailMetricsCollectorTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /**
         * @param since Time when metrics were collected last time.
         * @param res Response.
         * @param metrics Metrics.
         * @param collectNotSqlMetrics When {@code true} collect metrics for not SQL queries only.
         */
        private static void aggregateMetrics(
            long since, Map<GridCacheQueryDetailMetricsKey,
            GridCacheQueryDetailMetricsAdapter> res,
            Collection<GridCacheQueryDetailMetricsAdapter> metrics,
            boolean collectNotSqlMetrics
        ) {
            for (GridCacheQueryDetailMetricsAdapter m : metrics) {
                if (m.lastStartTime() > since) {
                    GridCacheQueryDetailMetricsKey key = m.key();

                    if (collectNotSqlMetrics && (key.getQueryType() == SQL || key.getQueryType() == SQL_FIELDS))
                        continue;

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

                    aggregateMetrics(arg.getSince(), aggMetrics, cache.context().queries().detailMetrics(), true);
                }
            }

            GridQueryIndexing indexing = ignite.context().query().getIndexing();

            if (indexing instanceof IgniteH2Indexing) {
                Collection<QueryHistoryMetrics> metrics = ((IgniteH2Indexing)indexing)
                    .runningQueryManager().queryHistoryMetrics().values();

                for (QueryHistoryMetrics m : metrics) {
                    GridCacheQueryDetailMetricsKey key = new GridCacheQueryDetailMetricsKey(SQL_FIELDS, m.query());

                    GridCacheQueryDetailMetricsAdapter oldMetrics = aggMetrics.get(key);

                    GridCacheQueryDetailMetricsAdapter total = oldMetrics != null
                        ? new GridCacheQueryDetailMetricsAdapter(
                            SQL_FIELDS, m.query(),
                            null,
                            oldMetrics.executions() + (int)m.executions(),
                            oldMetrics.completions() + (int)(m.executions() - m.failures()),
                            oldMetrics.failures() + (int)m.failures(),
                            Long.min(oldMetrics.minimumTime(), m.minimumTime()),
                            Long.max(oldMetrics.maximumTime(), m.maximumTime()),
                            oldMetrics.totalTime(),
                            Long.max(oldMetrics.lastStartTime(), m.lastStartTime()),
                            key)
                        : new GridCacheQueryDetailMetricsAdapter(
                            SQL_FIELDS,
                            m.query(),
                            null,
                            (int)m.executions(),
                            (int)(m.executions() - m.failures()),
                            (int)m.failures(),
                            m.minimumTime(),
                            m.maximumTime(),
                            0L,
                            m.lastStartTime(),
                            key);

                    aggMetrics.put(key, total);
                }
            }

            return new ArrayList<>(aggMetrics.values());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorQueryHistoryMetricsCollectorJob.class, this);
        }
    }
}
