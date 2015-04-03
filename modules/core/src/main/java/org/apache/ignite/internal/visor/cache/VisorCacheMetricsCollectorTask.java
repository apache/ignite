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
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.visor.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Task that collect cache metrics from all nodes.
 */
@GridInternal
public class VisorCacheMetricsCollectorTask extends VisorMultiNodeTask<IgniteBiTuple<Boolean, Collection<String>>,
    Iterable<VisorCacheAggregatedMetrics>, Map<String, VisorCacheMetrics>> {
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
        extends VisorJob<IgniteBiTuple<Boolean, Collection<String>>, Map<String, VisorCacheMetrics>> {
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
        @Override protected Map<String, VisorCacheMetrics> run(final IgniteBiTuple<Boolean, Collection<String>> arg) {
            assert arg != null;

            Boolean showSysCaches = arg.get1();

            assert showSysCaches != null;

            Collection<String> cacheNames = arg.get2();

            assert cacheNames != null;

            GridCacheProcessor cacheProcessor = ignite.context().cache();

            Collection<GridCacheAdapter<?, ?>> caches = cacheProcessor.internalCaches();

            Map<String, VisorCacheMetrics> res = U.newHashMap(caches.size());

            boolean allCaches = cacheNames.isEmpty();

            for (GridCacheAdapter ca : caches) {
                if (ca.context().started()) {
                    String name = ca.name();

                    if ((showSysCaches && cacheProcessor.systemCache(name)) || allCaches || cacheNames.contains(name))
                        res.put(name, VisorCacheMetrics.from(ca));
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
