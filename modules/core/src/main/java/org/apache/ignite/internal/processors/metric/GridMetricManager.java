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

package org.apache.ignite.internal.processors.metric;

import java.io.Serializable;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.processors.metric.impl.DoubleMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.metric.IgniteMetrics;
import org.apache.ignite.metric.LongValueMetric;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.ReadOnlyMetricManager;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_PHY_RAM;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.customName;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.fromFullName;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.util.IgniteUtils.notifyListeners;

/**
 * This manager should provide {@link ReadOnlyMetricManager} for each configured {@link MetricExporterSpi}.
 *
 * @see MetricExporterSpi
 * @see MetricRegistry
 */
public class GridMetricManager extends GridManagerAdapter<MetricExporterSpi> implements ReadOnlyMetricManager {
    /** Metrics update frequency. */
    private static final long METRICS_UPDATE_FREQ = 3000;

    /** System metrics prefix. */
    public static final String SYS_METRICS = "sys";

    /** Ignite node metrics prefix. */
    public static final String IGNITE_METRICS = "ignite";

    /** Partition map exchange metrics prefix. */
    public static final String PME_METRICS = "pme";

    /** Cluster metrics prefix. */
    public static final String CLUSTER_METRICS = "cluster";

    /** Client metrics prefix. */
    public static final String CLIENT_CONNECTOR_METRICS = metricName("client", "connector");

    /** Transaction metrics prefix. */
    public static final String TX_METRICS = "tx";

    /** GC CPU load metric name. */
    public static final String GC_CPU_LOAD = "GcCpuLoad";

    /** GC CPU load metric description. */
    public static final String GC_CPU_LOAD_DESCRIPTION = "GC CPU load.";

    /** CPU load metric name. */
    public static final String CPU_LOAD = "CpuLoad";

    /** CPU load metric description. */
    public static final String CPU_LOAD_DESCRIPTION = "CPU load.";

    /** Up time metric name. */
    public static final String UP_TIME = "UpTime";

    /** Thread count metric name. */
    public static final String THREAD_CNT = "ThreadCount";

    /** Peak thread count metric name. */
    public static final String PEAK_THREAD_CNT = "PeakThreadCount";

    /** Total started thread count metric name. */
    public static final String TOTAL_STARTED_THREAD_CNT = "TotalStartedThreadCount";

    /** Daemon thread count metric name. */
    public static final String DAEMON_THREAD_CNT = "DaemonThreadCount";

    /** PME duration metric name. */
    public static final String PME_DURATION = "Duration";

    /** PME cache operations blocked duration metric name. */
    public static final String PME_OPS_BLOCKED_DURATION = "CacheOperationsBlockedDuration";

    /** Histogram of PME durations metric name. */
    public static final String PME_DURATION_HISTOGRAM = "DurationHistogram";

    /** Histogram of blocking PME durations metric name. */
    public static final String PME_OPS_BLOCKED_DURATION_HISTOGRAM = "CacheOperationsBlockedDurationHistogram";

    /** Whether cluster is in fully rebalanced state metric name. */
    public static final String REBALANCED = "Rebalanced";

    /** Custom metrics registry name. */
    public static final String CUSTOM_METRICS = "custom";

    /** JVM interface to memory consumption info */
    private static final MemoryMXBean mem = ManagementFactory.getMemoryMXBean();

    /** */
    private static final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

    /** */
    private static final RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();

    /** */
    private static final ThreadMXBean threads = ManagementFactory.getThreadMXBean();

    /** */
    private static final Collection<GarbageCollectorMXBean> gc = ManagementFactory.getGarbageCollectorMXBeans();

    /** Prefix for {@link HitRateMetric} configuration property name. */
    public static final String HITRATE_CFG_PREFIX = metricName("metrics", "hitrate");

    /** Prefix for {@link HistogramMetric} configuration property name. */
    public static final String HISTOGRAM_CFG_PREFIX = metricName("metrics", "histogram");

    /** Registered metrics registries. */
    private final ConcurrentHashMap<String, ReadOnlyMetricRegistry> registries = new ConcurrentHashMap<>();

    /** Metric registry creation listeners. */
    private final List<Consumer<ReadOnlyMetricRegistry>> metricRegCreationLsnrs = new CopyOnWriteArrayList<>();

    /** Metric registry remove listeners. */
    private final List<Consumer<ReadOnlyMetricRegistry>> metricRegRemoveLsnrs = new CopyOnWriteArrayList<>();

    /** Read-only metastorage. */
    private volatile ReadableDistributedMetaStorage roMetastorage;

    /** Metastorage with the write access. */
    private volatile DistributedMetaStorage metastorage;

    /** Metrics update worker. */
    private GridTimeoutProcessor.CancelableTask metricsUpdateTask;

    /** GC CPU load. */
    private final DoubleMetricImpl gcCpuLoad;

    /** CPU load. */
    private final DoubleMetricImpl cpuLoad;

    /** Heap memory metrics. */
    private final MemoryUsageMetrics heap;

    /** Nonheap memory metrics. */
    private final MemoryUsageMetrics nonHeap;

    /** */
    private final SunOperatingSystemMXBeanAccessor sunOs;

    /** */
    private final IgniteMetrics customMetrics;

    /**
     * @param ctx Kernal context.
     */
    public GridMetricManager(GridKernalContext ctx) {
        super(ctx, ((Supplier<MetricExporterSpi[]>)() -> {
            MetricExporterSpi[] spi = ctx.config().getMetricExporterSpi();

            if (!IgniteComponentType.INDEXING.inClassPath() && !IgniteComponentType.QUERY_ENGINE.inClassPath())
                return spi;

            MetricExporterSpi[] spiWithSql = new MetricExporterSpi[spi != null ? spi.length + 1 : 1];

            if (!F.isEmpty(spi))
                System.arraycopy(spi, 0, spiWithSql, 0, spi.length);

            spiWithSql[spiWithSql.length - 1] = new SqlViewMetricExporterSpi();

            return spiWithSql;
        }).get());

        sunOs = sunOperatingSystemMXBeanAccessor();

        ctx.addNodeAttribute(ATTR_PHY_RAM, totalSysMemory());

        heap = new MemoryUsageMetrics(SYS_METRICS, metricName("memory", "heap"));
        nonHeap = new MemoryUsageMetrics(SYS_METRICS, metricName("memory", "nonheap"));

        heap.update(mem.getHeapMemoryUsage());
        nonHeap.update(mem.getNonHeapMemoryUsage());

        MetricRegistryImpl sysreg = registry(SYS_METRICS);

        gcCpuLoad = sysreg.doubleMetric(GC_CPU_LOAD, GC_CPU_LOAD_DESCRIPTION);
        cpuLoad = sysreg.doubleMetric(CPU_LOAD, CPU_LOAD_DESCRIPTION);

        sysreg.register("SystemLoadAverage", os::getSystemLoadAverage, Double.class, null);
        sysreg.register(UP_TIME, rt::getUptime, null);
        sysreg.register(THREAD_CNT, threads::getThreadCount, null);
        sysreg.register(PEAK_THREAD_CNT, threads::getPeakThreadCount, null);
        sysreg.register(TOTAL_STARTED_THREAD_CNT, threads::getTotalStartedThreadCount, null);
        sysreg.register(DAEMON_THREAD_CNT, threads::getDaemonThreadCount, null);
        sysreg.register("CurrentThreadCpuTime", threads::getCurrentThreadCpuTime, null);
        sysreg.register("CurrentThreadUserTime", threads::getCurrentThreadUserTime, null);

        MetricRegistryImpl pmeReg = registry(PME_METRICS);

        long[] pmeBounds = new long[] {500, 1000, 5000, 30000};

        pmeReg.histogram(PME_DURATION_HISTOGRAM, pmeBounds,
            "Histogram of PME durations in milliseconds.");

        pmeReg.histogram(PME_OPS_BLOCKED_DURATION_HISTOGRAM, pmeBounds,
            "Histogram of cache operations blocked PME durations in milliseconds.");

        customMetrics = new CustomMetricsImpl();
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() {
        metricsUpdateTask = ctx.timeout().schedule(new MetricsUpdater(), METRICS_UPDATE_FREQ, METRICS_UPDATE_FREQ);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        for (MetricExporterSpi spi : getSpis())
            spi.setMetricRegistry(this);

        startSpi();

        // In case standalone kernal start.
        if (ctx.internalSubscriptionProcessor() == null)
            return;

        ctx.internalSubscriptionProcessor().registerDistributedMetastorageListener(
            new DistributedMetastorageLifecycleListener() {
                /** {@inheritDoc} */
                @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
                    roMetastorage = metastorage;

                    try {
                        metastorage.iterate(HITRATE_CFG_PREFIX, (name, val) -> onHitRateConfigChanged(
                            name.substring(HITRATE_CFG_PREFIX.length() + 1), (Long)val));

                        metastorage.iterate(HISTOGRAM_CFG_PREFIX, (name, val) -> onHistogramConfigChanged(
                            name.substring(HISTOGRAM_CFG_PREFIX.length() + 1), (long[])val));
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }

                    metastorage.listen(n -> n.startsWith(HITRATE_CFG_PREFIX),
                        (name, oldVal, newVal) -> onHitRateConfigChanged(
                            name.substring(HITRATE_CFG_PREFIX.length() + 1), (Long)newVal));

                    metastorage.listen(n -> n.startsWith(HISTOGRAM_CFG_PREFIX),
                        (name, oldVal, newVal) -> onHistogramConfigChanged(
                            name.substring(HISTOGRAM_CFG_PREFIX.length() + 1), (long[])newVal));
                }

                /** {@inheritDoc} */
                @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
                    GridMetricManager.this.metastorage = metastorage;
                }
            });
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();

        // Stop discovery worker and metrics updater.
        U.closeQuiet(metricsUpdateTask);
    }

    /**
     * Gets or creates metric registry.
     *
     * @param name Group name.
     * @return Group of metrics.
     */
    public MetricRegistryImpl registry(String name) {
        boolean custom = name.startsWith(CUSTOM_METRICS);

        return (MetricRegistryImpl)registries.computeIfAbsent(name, n -> {
            MetricRegistryImpl mreg = new MetricRegistryImpl(name,
                custom ? null : mname -> readFromMetastorage(metricName(HITRATE_CFG_PREFIX, mname)),
                custom ? null : mname -> readFromMetastorage(metricName(HISTOGRAM_CFG_PREFIX, mname)),
                log);

            notifyListeners(mreg, metricRegCreationLsnrs, log);

            return mreg;
        });
    }

    /**
     * Reads value from {@link #roMetastorage}.
     *
     * @param key Key.
     * @param <T> Key type.
     * @return Value or {@code null} if not found.
     */
    private <T extends Serializable> T readFromMetastorage(String key) {
        if (roMetastorage == null)
            return null;

        try {
            return roMetastorage.read(key);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** @return Custom metrics. */
    public IgniteMetrics custom() {
        return customMetrics;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<ReadOnlyMetricRegistry> iterator() {
        return registries.values().iterator();
    }

    /** {@inheritDoc} */
    @Override public void addMetricRegistryCreationListener(Consumer<ReadOnlyMetricRegistry> lsnr) {
        metricRegCreationLsnrs.add(lsnr);
    }

    /** {@inheritDoc} */
    @Override public void addMetricRegistryRemoveListener(Consumer<ReadOnlyMetricRegistry> lsnr) {
        metricRegRemoveLsnrs.add(lsnr);
    }

    /**
     * Removes metric registry.
     *
     * @param regName Metric registry name.
     */
    public void remove(String regName) {
        remove(regName, true);
    }

    /**
     * Removes metric registry.
     *
     * @param regName Metric registry name.
     * @param removeCfg {@code True} if remove metric configurations.
     */
    public void remove(String regName, boolean removeCfg) {
        GridCompoundFuture opsFut = new GridCompoundFuture<>();

        registries.computeIfPresent(regName, (key, mreg) -> {
            notifyListeners(mreg, metricRegRemoveLsnrs, log);

            if (!removeCfg)
                return null;

            DistributedMetaStorage metastorage0 = metastorage;

            if (metastorage0 == null)
                return null;

            try {
                for (Metric m : mreg) {
                    if (m instanceof HitRateMetric)
                        opsFut.add(metastorage0.removeAsync(metricName(HITRATE_CFG_PREFIX, m.name())));
                    else if (m instanceof HistogramMetric)
                        opsFut.add(metastorage0.removeAsync(metricName(HISTOGRAM_CFG_PREFIX, m.name())));
                }
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            return null;
        });

        try {
            opsFut.markInitialized();
            opsFut.get();
        }
        catch (NodeStoppingException ignored) {
            // No-op.
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to remove metrics configuration.", e);
        }
    }

    /**
     * Change {@link HitRateMetric} configuration if it exists.
     *
     * @param name Metric name.
     * @param rateTimeInterval New rate time interval.
     * @throws IgniteCheckedException If write of configuration failed.
     * @see HitRateMetric#reset(long, int)
     */
    public void configureHitRate(String name, long rateTimeInterval) throws IgniteCheckedException {
        A.notNullOrEmpty(name, "name");
        A.ensure(rateTimeInterval > 0, "rateTimeInterval should be positive");
        A.notNull(metastorage, "Metastorage not ready. Node not started?");

        if (ctx.isStopping())
            throw new NodeStoppingException("Operation has been cancelled (node is stopping)");

        ensureMetricRegistered(name, HitRateMetric.class);

        metastorage.write(metricName(HITRATE_CFG_PREFIX, name), rateTimeInterval);
    }

    /**
     * Stores {@link HistogramMetric} configuration in metastorage.
     *
     * @param name Metric name.
     * @param bounds New bounds.
     * @throws IgniteCheckedException If write of configuration failed.
     */
    public void configureHistogram(String name, long[] bounds) throws IgniteCheckedException {
        A.notNullOrEmpty(name, "name");
        A.notEmpty(bounds, "bounds");
        A.notNull(metastorage, "Metastorage not ready. Node not started?");

        if (ctx.isStopping())
            throw new NodeStoppingException("Operation has been cancelled (node is stopping)");

        ensureMetricRegistered(name, HistogramMetric.class);

        metastorage.write(metricName(HISTOGRAM_CFG_PREFIX, name), bounds);
    }

    /**
     * Change {@link HitRateMetric} instance configuration.
     *
     * @param name Metric name.
     * @param rateTimeInterval New rateTimeInterval.
     * @see HitRateMetric#reset(long)
     */
    private void onHitRateConfigChanged(String name, @Nullable Long rateTimeInterval) {
        if (rateTimeInterval == null)
            return;

        A.ensure(rateTimeInterval > 0, "rateTimeInterval should be positive");

        HitRateMetric m = find(name, HitRateMetric.class);

        if (m == null)
            return;

        m.reset(rateTimeInterval);
    }

    /**
     * Change {@link HistogramMetric} instance configuration.
     *
     * @param name Metric name.
     * @param bounds New bounds.
     */
    private void onHistogramConfigChanged(String name, @Nullable long[] bounds) {
        if (bounds == null)
            return;

        ConfigurableHistogramMetric m = find(name, ConfigurableHistogramMetric.class);

        if (m == null)
            return;

        try {
            m.bounds(bounds);
        }
        catch (RuntimeException e) {
            // Can't throw exceptions here since method is invoked by metastorage listener.
            log.error("Error during histogram bounds reconfiguration", e);
        }
    }

    /**
     * @param name Metric name.
     * @param type Metric type.
     * @return Metric.
     */
    public <T extends Metric> T find(String name, Class<T> type) {
        A.notNull(name, "name");

        T2<String, String> splitted = fromFullName(name);

        MetricRegistry mreg = (MetricRegistry)registries.get(splitted.get1());

        if (mreg == null) {
            if (log.isInfoEnabled())
                log.info("Metric registry not found[registry=" + splitted.get1() + ']');

            return null;
        }

        Metric m = mreg.findMetric(splitted.get2());

        if (m == null) {
            if (log.isInfoEnabled())
                log.info("Metric not found[registry=" + splitted.get1() + ", metricName=" + splitted.get2() + ']');

            return null;
        }

        if (!type.isAssignableFrom(m.getClass())) {
            log.error("Metric '" + name + "' has wrong type[type=" + m.getClass().getSimpleName() + ']');

            return null;
        }

        return (T)m;
    }

    /**
     * @return Memory usage of non-heap memory.
     */
    public MemoryUsage nonHeapMemoryUsage() {
        // Workaround of exception in WebSphere.
        // We received the following exception:
        // java.lang.IllegalArgumentException: used value cannot be larger than the committed value
        // at java.lang.management.MemoryUsage.<init>(MemoryUsage.java:105)
        // at com.ibm.lang.management.MemoryMXBeanImpl.getNonHeapMemoryUsageImpl(Native Method)
        // at com.ibm.lang.management.MemoryMXBeanImpl.getNonHeapMemoryUsage(MemoryMXBeanImpl.java:143)
        // at org.apache.ignite.spi.metrics.jdk.GridJdkLocalMetricsSpi.getMetrics(GridJdkLocalMetricsSpi.java:242)
        //
        // We so had to workaround this with exception handling, because we can not control classes from WebSphere.
        try {
            return mem.getNonHeapMemoryUsage();
        }
        catch (IllegalArgumentException ignored) {
            return new MemoryUsage(0, 0, 0, 0);
        }
    }

    /**
     * Returns the current memory usage of the heap.
     * @return Memory usage or fake value with zero in case there was exception during take of metrics.
     */
    public MemoryUsage heapMemoryUsage() {
        // Catch exception here to allow discovery proceed even if metrics are not available
        // java.lang.IllegalArgumentException: committed = 5274103808 should be < max = 5274095616
        // at java.lang.management.MemoryUsage.<init>(Unknown Source)
        try {
            return mem.getHeapMemoryUsage();
        }
        catch (IllegalArgumentException ignored) {
            return new MemoryUsage(0, 0, 0, 0);
        }
    }

    /** */
    private <T extends Metric> void ensureMetricRegistered(String name, Class<T> cls) {
        if (find(name, cls) == null)
            throw new IgniteException("Failed to find registered metric with specified name [metricName=" + name + ']');
    }

    /**
     * @return Total system memory.
     */
    private long totalSysMemory() {
        try {
            return sunOs.getTotalPhysicalMemorySize();
        }
        catch (RuntimeException ignored) {
            return -1;
        }
    }

    /** @return Accessor for {@link com.sun.management.OperatingSystemMXBean}. */
    private SunOperatingSystemMXBeanAccessor sunOperatingSystemMXBeanAccessor() {
        try {
            if (os instanceof com.sun.management.OperatingSystemMXBean) {
                com.sun.management.OperatingSystemMXBean sunOs = (com.sun.management.OperatingSystemMXBean)os;

                return new SunOperatingSystemMXBeanAccessor() {
                    @Override public long getProcessCpuTime() {
                        return sunOs.getProcessCpuTime();
                    }

                    @Override public long getTotalPhysicalMemorySize() {
                        return sunOs.getTotalPhysicalMemorySize();
                    }
                };
            }
        }
        catch (@SuppressWarnings("ErrorNotRethrown") NoClassDefFoundError ignored) {
            log.warning("The 'com.sun.management.OperatingSystemMXBean' class is not available for class loader. " +
                "System/JVM memory and CPU statistics may be not available.");
        }

        return new SunOperatingSystemMXBeanAccessor() {
            @Override public long getProcessCpuTime() {
                return U.<Long>property(os, "processCpuTime");
            }

            @Override public long getTotalPhysicalMemorySize() {
                return U.<Long>property(os, "totalPhysicalMemorySize");
            }
        };
    }

    /** */
    private class MetricsUpdater implements Runnable {
        /** */
        private long prevGcTime = -1;

        /** */
        private long prevCpuTime = -1;

        /** {@inheritDoc} */
        @Override public void run() {
            heap.update(heapMemoryUsage());
            nonHeap.update(nonHeapMemoryUsage());

            gcCpuLoad.value(getGcCpuLoad());
            cpuLoad.value(getCpuLoad());
        }

        /**
         * @return GC CPU load.
         */
        private double getGcCpuLoad() {
            long gcTime = 0;

            for (GarbageCollectorMXBean bean : gc) {
                long colTime = bean.getCollectionTime();

                if (colTime > 0)
                    gcTime += colTime;
            }

            gcTime /= os.getAvailableProcessors();

            double gc = 0;

            if (prevGcTime > 0) {
                long gcTimeDiff = gcTime - prevGcTime;

                gc = (double)gcTimeDiff / METRICS_UPDATE_FREQ;
            }

            prevGcTime = gcTime;

            return gc;
        }

        /**
         * @return CPU load.
         */
        private double getCpuLoad() {
            long cpuTime;

            try {
                cpuTime = sunOs.getProcessCpuTime();
            }
            catch (RuntimeException ignored) {
                return -1;
            }

            // Method reports time in nanoseconds across all processors.
            cpuTime /= 1000000 * os.getAvailableProcessors();

            double cpu = 0;

            if (prevCpuTime > 0) {
                long cpuTimeDiff = cpuTime - prevCpuTime;

                // CPU load could go higher than 100% because calculating of cpuTimeDiff also takes some time.
                cpu = Math.min(1.0, (double)cpuTimeDiff / METRICS_UPDATE_FREQ);
            }

            prevCpuTime = cpuTime;

            return cpu;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MetricsUpdater.class, this, super.toString());
        }
    }

    /** Memory usage metrics. */
    public class MemoryUsageMetrics {
        /** @see MemoryUsage#getInit() */
        private final LongValueMetric init;

        /** @see MemoryUsage#getUsed() */
        private final LongValueMetric used;

        /** @see MemoryUsage#getCommitted() */
        private final LongValueMetric committed;

        /** @see MemoryUsage#getMax() */
        private final LongValueMetric max;

        /**
         * @param grp Metric registry.
         * @param metricNamePrefix Metric name prefix.
         */
        public MemoryUsageMetrics(String grp, String metricNamePrefix) {
            MetricRegistry mreg = registry(grp);

            this.init = mreg.longMetric(metricName(metricNamePrefix, "init"), null);
            this.used = mreg.longMetric(metricName(metricNamePrefix, "used"), null);
            this.committed = mreg.longMetric(metricName(metricNamePrefix, "committed"), null);
            this.max = mreg.longMetric(metricName(metricNamePrefix, "max"), null);
        }

        /** Updates metric to the provided values. */
        public void update(MemoryUsage usage) {
            init.set(usage.getInit());
            used.set(usage.getUsed());
            committed.set(usage.getCommitted());
            max.set(usage.getMax());
        }
    }

    /** Accessor for {@link com.sun.management.OperatingSystemMXBean} methods. */
    private interface SunOperatingSystemMXBeanAccessor {
        /** @see com.sun.management.OperatingSystemMXBean#getProcessCpuTime() */
        long getProcessCpuTime();

        /** @see com.sun.management.OperatingSystemMXBean#getTotalPhysicalMemorySize() */
        long getTotalPhysicalMemorySize();
    }

    /** Custom metrics impl. */
    private class CustomMetricsImpl implements IgniteMetrics {
        /** {@inheritDoc} */
        @Override public MetricRegistry getOrCreate(String registryName) {
            return registry(customName(registryName));
        }

        /** {@inheritDoc} */
        @Override public void remove(String registryName) {
            GridMetricManager.this.remove(customName(registryName), false);
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<ReadOnlyMetricRegistry> iterator() {
            return F.viewReadOnly(registries.values(), r -> r, r -> MetricUtils.customMetric(r.name())).iterator();
        }
    }
}
