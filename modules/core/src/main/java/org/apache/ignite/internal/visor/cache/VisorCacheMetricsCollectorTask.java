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

package org.apache.ignite.internal.visor.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.util.VisorTaskUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Task that collect cache metrics from all nodes.
 */
@GridInternal
public class VisorCacheMetricsCollectorTask extends VisorMultiNodeTask<VisorCacheMetricsCollectorTaskArg,
    Iterable<VisorCacheAggregatedMetrics>, Collection<VisorCacheMetrics>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheMetricsCollectorJob job(VisorCacheMetricsCollectorTaskArg arg) {
        return new VisorCacheMetricsCollectorJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Iterable<VisorCacheAggregatedMetrics> reduce0(List<ComputeJobResult> results) {
        Map<String, VisorCacheAggregatedMetrics> grpAggrMetrics = U.newHashMap(results.size());

        for (ComputeJobResult res : results) {
            if (res.getException() == null) {
                Collection<VisorCacheMetrics> cms = res.getData();

                for (VisorCacheMetrics cm : cms) {
                    VisorCacheAggregatedMetrics am = grpAggrMetrics.get(cm.getName());

                    if (am == null) {
                        am = new VisorCacheAggregatedMetrics(cm);

                        grpAggrMetrics.put(cm.getName(), am);
                    }

                    am.getMetrics().put(res.getNode().id(), cm);
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
        extends VisorJob<VisorCacheMetricsCollectorTaskArg, Collection<VisorCacheMetrics>> {

        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Whether to collect metrics for all caches or for specified cache name only.
         * @param debug Debug flag.
         */
        private VisorCacheMetricsCollectorJob(VisorCacheMetricsCollectorTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Collection<VisorCacheMetrics> run(final VisorCacheMetricsCollectorTaskArg arg) {
            assert arg != null;

            boolean showSysCaches = arg.isShowSystemCaches();

            Collection<String> cacheNames = arg.getCacheNames();

            assert cacheNames != null;

            GridCacheProcessor cacheProcessor = ignite.context().cache();

            Collection<IgniteCacheProxy<?, ?>> caches = cacheProcessor.jcaches();

            Collection<VisorCacheMetrics> res = new ArrayList<>(caches.size());

            boolean allCaches = cacheNames.isEmpty();

            for (IgniteCacheProxy ca : caches) {
                String cacheName = ca.getName();

                if (!VisorTaskUtils.isRestartingCache(ignite, cacheName)) {
                    GridCacheContext ctx = ca.context();

                    if (ctx.started() && (ctx.affinityNode() || ctx.isNear())) {
                        VisorCacheMetrics cm = new VisorCacheMetrics(ignite, cacheName);

                        if ((allCaches || cacheNames.contains(cacheName)) && (showSysCaches || !cm.isSystem()))
                            res.add(cm);
                    }
                }
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheMetricsCollectorJob.class, this);
        }
    }
}
