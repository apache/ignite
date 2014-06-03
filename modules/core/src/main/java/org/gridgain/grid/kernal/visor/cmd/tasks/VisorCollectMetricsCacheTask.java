/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.kernal.visor.cmd.dto.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Task that cache metrics from all nodes.
 */
@GridInternal
public class VisorCollectMetricsCacheTask extends VisorMultiNodeTask<VisorCollectMetricsCacheTask.VisorCollectMetricsCacheArg,
    Iterable<VisorCacheAggregatedMetrics>,
    Collection<VisorCacheMetrics>> {
    /**
     * Arguments for {@link VisorCollectMetricsCacheTask}.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorCollectMetricsCacheArg extends VisorMultiNodeArg {
        /** */
        private static final long serialVersionUID = 0L;

        /** Collect metrics from all caches. */
        private final boolean all;

        /** Name of cache to collect metrics. */
        @Nullable private final String cacheName;

        public VisorCollectMetricsCacheArg(Set<UUID> nids, boolean all, @Nullable String cacheName) {
            super(nids);

            this.all = all;
            this.cacheName = cacheName;
        }
    }

    /**
     * Job that collect cache metrics from node.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorCacheMetricsJob extends VisorJob<VisorCollectMetricsCacheArg, Collection<VisorCacheMetrics>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Create job with given argument. */
        public VisorCacheMetricsJob(VisorCollectMetricsCacheArg arg) {
            super(arg);
        }

        @Override protected Collection<VisorCacheMetrics> run(VisorCollectMetricsCacheArg arg) throws GridException {
            Collection<? extends GridCache<?, ?>> caches = arg.all ? g.cachesx() : F.asList(g.cachex(arg.cacheName));

            if (caches != null) {
                Collection<VisorCacheMetrics> res = new ArrayList<>(caches.size());

                for (GridCache<?, ?> c : caches) {
                    GridNodeMetrics m = g.localNode().metrics();
                    GridCacheMetrics cm = c.metrics();
                    GridCacheQueryMetrics qm = c.queries().metrics();

                    res.add(new VisorCacheMetrics(
                        c.name(),
                        g.localNode().id(),
                        m.getTotalCpus(),
                        (double)m.getHeapMemoryUsed() / m.getHeapMemoryMaximum() * 100.0,
                        m.getCurrentCpuLoad() * 100.0,
                        m.getUpTime(),
                        caches.size(),
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
    }

    @Override protected VisorCacheMetricsJob job(VisorCollectMetricsCacheArg arg) {
        return new VisorCacheMetricsJob(arg);
    }

    @Nullable @Override public Iterable<VisorCacheAggregatedMetrics> reduce(List<GridComputeJobResult> results)
        throws GridException {
        Map<String, VisorCacheAggregatedMetrics> grpAggrMetrics = new HashMap<>();

        for (GridComputeJobResult res : results) {
            if (res.getException() == null && res.getData() instanceof Collection<?>) {
                Collection<VisorCacheMetrics> cms = res.getData();
                for (VisorCacheMetrics cm : cms) {

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
}
