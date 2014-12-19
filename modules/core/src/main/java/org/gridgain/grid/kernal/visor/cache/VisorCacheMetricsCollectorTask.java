/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cache;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Task that collect cache metrics from all nodes.
 */
@GridInternal
public class VisorCacheMetricsCollectorTask extends VisorMultiNodeTask<IgniteBiTuple<Boolean, String>,
    Iterable<VisorCacheAggregatedMetrics>, Map<String, VisorCacheMetrics>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheMetricsJob job(IgniteBiTuple<Boolean, String> arg) {
        return new VisorCacheMetricsJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Iterable<VisorCacheAggregatedMetrics> reduce0(List<ComputeJobResult> results)
        throws IgniteCheckedException {
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

        return grpAggrMetrics.values();
    }

    /**
     * Job that collect cache metrics from node.
     */
    private static class VisorCacheMetricsJob
        extends VisorJob<IgniteBiTuple<Boolean, String>, Map<String, VisorCacheMetrics>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Whether to collect metrics for all caches or for specified cache name only.
         * @param debug Debug flag.
         */
        private VisorCacheMetricsJob(IgniteBiTuple<Boolean, String> arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Map<String, VisorCacheMetrics>
            run(IgniteBiTuple<Boolean, String> arg) throws IgniteCheckedException {
            Collection<? extends GridCache<?, ?>> caches = arg.get1() ? g.cachesx() : F.asList(g.cachex(arg.get2()));

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
            return S.toString(VisorCacheMetricsJob.class, this);
        }
    }
}
