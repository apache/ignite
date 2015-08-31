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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Task that collect cache metrics from all nodes.
 */
@GridInternal
public class VisorCacheMetricsCollectorTask extends VisorMultiNodeTask<IgniteBiTuple<Boolean, Collection<String>>,
    Iterable<VisorCacheAggregatedMetrics>, Collection<VisorCacheMetrics>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheMetricsCollectorJob job(IgniteBiTuple<Boolean, Collection<String>> arg) {
        return new VisorCacheMetricsCollectorJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Iterable<VisorCacheAggregatedMetrics> reduce0(List<ComputeJobResult> results) {
        Map<String, VisorCacheAggregatedMetrics> grpAggrMetrics = U.newHashMap(results.size());

        for (ComputeJobResult res : results) {
            if (res.getException() == null) {
                Collection<VisorCacheMetrics> cms = res.getData();

                for (VisorCacheMetrics cm : cms) {
                    VisorCacheAggregatedMetrics am = grpAggrMetrics.get(cm.name());

                    if (am == null) {
                        am = VisorCacheAggregatedMetrics.from(cm);

                        grpAggrMetrics.put(cm.name(), am);
                    }

                    am.metrics().put(res.getNode().id(), cm);
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
        extends VisorJob<IgniteBiTuple<Boolean, Collection<String>>, Collection<VisorCacheMetrics>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Whether to collect metrics for all caches or for specified cache name only.
         * @param debug Debug flag.
         */
        private VisorCacheMetricsCollectorJob(IgniteBiTuple<Boolean, Collection<String>> arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Collection<VisorCacheMetrics> run(final IgniteBiTuple<Boolean, Collection<String>> arg) {
            assert arg != null;

            Boolean showSysCaches = arg.get1();

            assert showSysCaches != null;

            Collection<String> cacheNames = arg.get2();

            assert cacheNames != null;

            GridCacheProcessor cacheProcessor = ignite.context().cache();

            Collection<IgniteCacheProxy<?, ?>> caches = cacheProcessor.jcaches();

            Collection<VisorCacheMetrics> res = new ArrayList<>(caches.size());

            boolean allCaches = cacheNames.isEmpty();

            for (IgniteCacheProxy ca : caches) {
                if (ca.context().started()) {
                    String cacheName = ca.getName();

                    VisorCacheMetrics cm = VisorCacheMetrics.from(ignite, cacheName);

                    if ((allCaches || cacheNames.contains(cacheName)) && (showSysCaches || !cm.system()))
                        res.add(cm);
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