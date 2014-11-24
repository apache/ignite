/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Task that collect cache metrics from all nodes.
 */
@GridInternal
public class VisorCacheMetricsCollectorTask extends VisorMultiNodeTask<GridBiTuple<Boolean, String>,
    Iterable<VisorCacheAggregatedMetrics>, Collection<VisorCacheMetrics2>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheMetricsJob job(GridBiTuple<Boolean, String> arg) {
        return new VisorCacheMetricsJob(arg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Iterable<VisorCacheAggregatedMetrics> reduce(List<GridComputeJobResult> results)
        throws GridException {
        Map<String, VisorCacheAggregatedMetrics> grpAggrMetrics = new HashMap<>();

        for (GridComputeJobResult res : results) {
            if (res.getException() == null && res.getData() instanceof Collection<?>) {
                Collection<VisorCacheMetrics2> cms = res.getData();
                for (VisorCacheMetrics2 cm : cms) {

                    VisorCacheAggregatedMetrics am = grpAggrMetrics.get(cm.cacheName());

                    if (am == null) {
                        am = new VisorCacheAggregatedMetrics(cm.cacheName());

                        grpAggrMetrics.put(cm.cacheName(), am);
                    }

                    am.nodes().add(cm.nodeId());
                    am.minSize(cm.size());
                    am.maxSize(cm.size());
                    am.lastRead(cm.lastRead());
                    am.lastWrite(cm.lastWrite());
                    am.minHits(cm.hits());
                    am.maxHits(cm.hits());
                    am.minMisses(cm.misses());
                    am.maxMisses(cm.misses());
                    am.minReads(cm.reads());
                    am.maxReads(cm.reads());
                    am.minWrites(cm.writes());
                    am.maxWrites(cm.writes());
                    am.metrics().add(cm);

                    // Partial aggregation of averages.
                    am.avgReads(am.avgReads() + cm.reads());
                    am.avgWrites(am.avgWrites() + cm.writes());
                    am.avgMisses(am.avgMisses() + cm.misses());
                    am.avgHits(am.avgHits() + cm.hits());
                    am.avgSize(am.avgSize() + cm.size());

                    // Aggregate query metrics data
                    VisorCacheQueryMetrics qm = cm.queryMetrics();
                    VisorCacheQueryAggregatedMetrics aqm = am.queryMetrics();

                    aqm.minTime(qm.minTime());
                    aqm.maxTime(qm.maxTime());
                    aqm.totalTime((long)(aqm.totalTime() + (qm.avgTime() * qm.execs())));
                    aqm.execs(aqm.execs() + qm.execs());
                    aqm.fails(aqm.fails() + qm.fails());
                }
            }
        }

        Collection<VisorCacheAggregatedMetrics> aggrMetrics = grpAggrMetrics.values();

        // Final aggregation of averages.
        for (VisorCacheAggregatedMetrics metric : aggrMetrics) {
            int sz = metric.nodes().size();

            metric.avgSize(metric.avgSize() / sz);
            metric.avgHits(metric.avgHits() / sz);
            metric.avgMisses(metric.avgMisses() / sz);
            metric.avgReads(metric.avgReads() / sz);
            metric.avgWrites(metric.avgWrites() / sz);

            VisorCacheQueryAggregatedMetrics aqm = metric.queryMetrics();

            aqm.avgTime(aqm.execs() > 0 ? (double)aqm.totalTime() / aqm.execs() : 0.0);
        }

        return aggrMetrics;
    }

    /**
     * Job that collect cache metrics from node.
     */
    private static class VisorCacheMetricsJob extends VisorJob<GridBiTuple<Boolean, String>, Collection<VisorCacheMetrics2>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Whether to collect metrics for all caches or for specified cache name only.
         */
        private VisorCacheMetricsJob(GridBiTuple<Boolean, String> arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected Collection<VisorCacheMetrics2> run(GridBiTuple<Boolean, String> arg) throws GridException {
            Collection<? extends GridCache<?, ?>> caches = arg.get1() ? g.cachesx() : F.asList(g.cachex(arg.get2()));

            if (caches != null) {
                Collection<VisorCacheMetrics2> res = new ArrayList<>(caches.size());

                for (GridCache<?, ?> c : caches) {
                    GridNodeMetrics m = g.localNode().metrics();
                    GridCacheMetrics cm = c.metrics();
                    GridCacheQueryMetrics qm = c.queries().metrics();

                    res.add(new VisorCacheMetrics2(
                        c.name(),
                        g.localNode().id(),
                        m.getTotalCpus(),
                        (double)m.getHeapMemoryUsed() / m.getHeapMemoryMaximum() * 100.0,
                        m.getCurrentCpuLoad() * 100.0,
                        m.getUpTime(),
                        c.size(),
                        cm.readTime(),
                        cm.writeTime(),
                        cm.hits(),
                        cm.misses(),
                        cm.reads(),
                        cm.writes(),
                        new VisorCacheQueryMetrics(qm.minimumTime(), qm.maximumTime(), qm.averageTime(),
                            qm.executions(), qm.fails())
                    ));
                }

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
