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
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Task that runs on all nodes and returns cache metrics.
 */
@GridInternal
public class VisorCollectMetricsCacheTask extends VisorMultiNodeTask<VisorCollectMetricsCacheTask.VisorCollectMetricsCacheArg,
    Iterable<VisorCollectMetricsCacheTask.VisorCacheAggregatedMetrics>,
    Collection<VisorCollectMetricsCacheTask.VisorCacheMetrics>> {
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

        public VisorCollectMetricsCacheArg(Set<UUID> ids, boolean all, @Nullable String cacheName) {
            super(ids);

            this.all = all;
            this.cacheName = cacheName;
        }

        /**
         * @return Collect metrics from all caches.
         */
        public boolean all() {
            return all;
        }

        /**
         * @return Name of cache to collect metrics.
         */
        public String cacheName() {
            return cacheName;
        }
    }

    /**
     * Cache query metrics.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorCacheQueryMetrics implements Serializable{
        /** */
        private static final long serialVersionUID = 0L;

        private final long minTime;
        private final long maxTime;
        private final double avgTime;
        private final int execs;
        private final int fails;

        public VisorCacheQueryMetrics(long minTime, long maxTime, double avgTime, int execs, int fails) {
            this.minTime = minTime;
            this.maxTime = maxTime;
            this.avgTime = avgTime;
            this.execs = execs;
            this.fails = fails;
        }

        /**
         * @return Min time.
         */
        public long minTime() {
            return minTime;
        }

        /**
         * @return Max time.
         */
        public long maxTime() {
            return maxTime;
        }

        /**
         * @return Avg time.
         */
        public double avgTime() {
            return avgTime;
        }

        /**
         * @return Execs.
         */
        public int execs() {
            return execs;
        }

        /**
         * @return Fails.
         */
        public int fails() {
            return fails;
        }
    }

    /**
     * Cache metrics.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorCacheMetrics implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final String cacheName;
        private final UUID nodeId;
        private final int cpus;
        private final double heapUsed;
        private final double cpuLoad;
        private final long upTime;
        private final int size;
        private final long lastRead;
        private final long lastWrite;
        private final int hits;
        private final int misses;
        private final int reads;
        private final int writes;
        private final VisorCacheQueryMetrics qryMetrics;

        public VisorCacheMetrics(String cacheName, UUID nodeId, int cpus, double heapUsed, double cpuLoad,
            long upTime,
            int size, long lastRead, long lastWrite, int hits, int misses, int reads, int writes,
            VisorCacheQueryMetrics qryMetrics) {
            this.cacheName = cacheName;
            this.nodeId = nodeId;
            this.cpus = cpus;
            this.heapUsed = heapUsed;
            this.cpuLoad = cpuLoad;
            this.upTime = upTime;
            this.size = size;
            this.lastRead = lastRead;
            this.lastWrite = lastWrite;
            this.hits = hits;
            this.misses = misses;
            this.reads = reads;
            this.writes = writes;
            this.qryMetrics = qryMetrics;
        }

        /**
         * @return Cache name.
         */
        public String cacheName() {
            return cacheName;
        }

        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Cpus.
         */
        public int cpus() {
            return cpus;
        }

        /**
         * @return Heap used.
         */
        public double heapUsed() {
            return heapUsed;
        }

        /**
         * @return Cpu load.
         */
        public double cpuLoad() {
            return cpuLoad;
        }

        /**
         * @return Up time.
         */
        public long upTime() {
            return upTime;
        }

        /**
         * @return Size.
         */
        public int size() {
            return size;
        }

        /**
         * @return Last read.
         */
        public long lastRead() {
            return lastRead;
        }

        /**
         * @return Last write.
         */
        public long lastWrite() {
            return lastWrite;
        }

        /**
         * @return Hits.
         */
        public int hits() {
            return hits;
        }

        /**
         * @return Misses.
         */
        public int misses() {
            return misses;
        }

        /**
         * @return Reads.
         */
        public int reads() {
            return reads;
        }

        /**
         * @return Writes.
         */
        public int writes() {
            return writes;
        }

        /**
         * @return Query metrics.
         */
        public VisorCacheQueryMetrics queryMetrics() {
            return qryMetrics;
        }
    }

    /**
     * Aggregated cache metrics.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorCacheQueryAggregatedMetrics implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private long minTime = Long.MAX_VALUE;
        private long maxTime = Long.MIN_VALUE;
        private double avgTime;
        private long totalTime;
        private int execs;
        private int fails;

        /**
         * @return Min time.
         */
        public long minTime() {
            return minTime;
        }

        /**
         * @param minTime New min time.
         */
        public void minTime(long minTime) {
            this.minTime = Math.min(this.minTime, minTime);
        }

        /**
         * @return Max time.
         */
        public long maxTime() {
            return maxTime;
        }

        /**
         * @param maxTime New max time.
         */
        public void maxTime(long maxTime) {
            this.maxTime = Math.max(this.maxTime, maxTime);
        }

        /**
         * @return Avg time.
         */
        public double avgTime() {
            return avgTime;
        }

        /**
         * @param avgTime New avg time.
         */
        public void avgTime(double avgTime) {
            this.avgTime = avgTime;
        }

        /**
         * @return Total time.
         */
        public long totalTime() {
            return totalTime;
        }

        /**
         * @param totalTime New total time.
         */
        public void totalTime(long totalTime) {
            this.totalTime = totalTime;
        }

        /**
         * @return Execs.
         */
        public int execs() {
            return execs;
        }

        /**
         * @param execs New execs.
         */
        public void execs(int execs) {
            this.execs = execs;
        }

        /**
         * @return Fails.
         */
        public int fails() {
            return fails;
        }

        /**
         * @param fails New fails.
         */
        public void fails(int fails) {
            this.fails = fails;
        }
    }

    /**
     * Aggregated cache metrics.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorCacheAggregatedMetrics implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final String cacheName;
        private Collection<UUID> nodes = new ArrayList<>();
        private int minSize = Integer.MAX_VALUE;
        private double avgSize;
        private int maxSize = Integer.MIN_VALUE;
        private long lastRead;
        private long lastWrite;
        private int minHits = Integer.MAX_VALUE;
        private double avgHits;
        private int maxHits = Integer.MIN_VALUE;
        private int minMisses = Integer.MAX_VALUE;
        private double avgMisses;
        private int maxMisses = Integer.MIN_VALUE;
        private int minReads = Integer.MAX_VALUE;
        private double avgReads;
        private int maxReads = Integer.MIN_VALUE;
        private int minWrites = Integer.MAX_VALUE;
        private double avgWrites;
        private int maxWrites = Integer.MIN_VALUE;
        private VisorCacheQueryAggregatedMetrics qryMetrics = new VisorCacheQueryAggregatedMetrics();
        private Collection<VisorCacheMetrics> metrics = new ArrayList<>();

        public VisorCacheAggregatedMetrics(String cacheName) {
            this.cacheName = cacheName;
        }

        /**
         * @return Cache name.
         */
        public String cacheName() {
            return cacheName;
        }

        /**
         * @return Nodes.
         */
        public Collection<UUID> nodes() {
            return nodes;
        }

        /**
         * @return Min size.
         */
        public int minSize() {
            return minSize;
        }

        /**
         * @param size New min size.
         */
        public void minSize(int size) {
            minSize = Math.min(minSize, size);
        }

        /**
         * @return Avg size.
         */
        public double avgSize() {
            return avgSize;
        }

        /**
         * @param avgSize New avg size.
         */
        public void avgSize(double avgSize) {
            this.avgSize = avgSize;
        }

        /**
         * @return Max size.
         */
        public int maxSize() {
            return maxSize;
        }

        /**
         * @param size New max size.
         */
        public void maxSize(int size) {
            maxSize = Math.max(maxSize, size);
        }

        /**
         * @return Last read.
         */
        public long lastRead() {
            return lastRead;
        }

        /**
         * @param lastRead New last read.
         */
        public void lastRead(long lastRead) {
            this.lastRead = Math.max(this.lastRead, lastRead);
        }

        /**
         * @return Last write.
         */
        public long lastWrite() {
            return lastWrite;
        }

        /**
         * @param lastWrite New last write.
         */
        public void lastWrite(long lastWrite) {
            this.lastWrite = Math.max(this.lastWrite, lastWrite);
        }

        /**
         * @return Min hits.
         */
        public int minHits() {
            return minHits;
        }

        /**
         * @param minHits New min hits.
         */
        public void minHits(int minHits) {
            this.minHits = Math.min(this.minHits, minHits);
        }

        /**
         * @return Avg hits.
         */
        public double avgHits() {
            return avgHits;
        }

        /**
         * @param avgHits New avg hits.
         */
        public void avgHits(double avgHits) {
            this.avgHits = avgHits;
        }

        /**
         * @return Max hits.
         */
        public int maxHits() {
            return maxHits;
        }

        /**
         * @param hits New max hits.
         */
        public void maxHits(int hits) {
            maxHits = Math.max(maxHits, hits);
        }

        /**
         * @return Min misses.
         */
        public int minMisses() {
            return minMisses;
        }

        /**
         * @param misses New min misses.
         */
        public void minMisses(int misses) {
            minMisses = Math.min(minMisses, misses);
        }

        /**
         * @return Avg misses.
         */
        public double avgMisses() {
            return avgMisses;
        }

        /**
         * @param avgMisses New avg misses.
         */
        public void avgMisses(double avgMisses) {
            this.avgMisses = avgMisses;
        }

        /**
         * @return Max misses.
         */
        public int maxMisses() {
            return maxMisses;
        }

        /**
         * @param maxMisses New max misses.
         */
        public void maxMisses(int maxMisses) {
            this.maxMisses = maxMisses;
        }

        /**
         * @return Min reads.
         */
        public int minReads() {
            return minReads;
        }

        /**
         * @param reads New min reads.
         */
        public void minReads(int reads) {
            minReads = Math.min(minReads, reads);
        }

        /**
         * @return Avg reads.
         */
        public double avgReads() {
            return avgReads;
        }

        /**
         * @param avgReads New avg reads.
         */
        public void avgReads(double avgReads) {
            this.avgReads = avgReads;
        }

        /**
         * @return Max reads.
         */
        public int maxReads() {
            return maxReads;
        }

        /**
         * @param reads New max reads.
         */
        public void maxReads(int reads) {
            maxReads = Math.max(maxReads, reads);
        }

        /**
         * @return Min writes.
         */
        public int minWrites() {
            return minWrites;
        }

        /**
         * @param writes New min writes.
         */
        public void minWrites(int writes) {
            minWrites = Math.min(minWrites, writes);
        }

        /**
         * @return Avg writes.
         */
        public double avgWrites() {
            return avgWrites;
        }

        /**
         * @param avgWrites New avg writes.
         */
        public void avgWrites(double avgWrites) {
            this.avgWrites = avgWrites;
        }

        /**
         * @return Max writes.
         */
        public int maxWrites() {
            return maxWrites;
        }

        /**
         * @param writes New max writes.
         */
        public void maxWrites(int writes) {
            maxWrites = Math.max(maxWrites, writes);
        }

        /**
         * @return Query metrics.
         */
        public VisorCacheQueryAggregatedMetrics queryMetrics() {
            return qryMetrics;
        }

        /**
         * @return Metrics.
         */
        public Collection<VisorCacheMetrics> metrics() {
            return metrics;
        }
    }

    /**
     * Aggregated cache metrics.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorCacheMetricsJob extends VisorJob<VisorCollectMetricsCacheArg, Collection<VisorCacheMetrics>> {
        /** */
        private static final long serialVersionUID = 0L;

        public VisorCacheMetricsJob(VisorCollectMetricsCacheArg arg) {
            super(arg);
        }

        @Override protected Collection<VisorCacheMetrics> run(VisorCollectMetricsCacheArg arg) throws GridException {
            Collection<? extends GridCache<?, ?>> caches = arg.all() ? g.cachesx(null) : F.asList(g.cachex(arg.cacheName()));

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

                    am.nodes.add(cm.nodeId());
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
