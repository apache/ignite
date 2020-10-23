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

package org.apache.ignite.internal.processors.metric.sources;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Collection;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistryBuilder;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.DoubleMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.IntGauge;
import org.apache.ignite.internal.processors.metric.impl.LongGauge;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * System metric source. System metrics are metrics that don't depend on particular Ignite components.
 */
public class SystemMetricSource extends AbstractMetricSource<SystemMetricSource.Holder> {
    /** Metrics update frequency. */
    private static final long METRICS_UPDATE_FREQ = 3000;

    /** System metrics prefix. */
    public static final String SYS_METRICS = "sys";

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

    /** Last data version metric name. */
    public static final String LAST_DATA_VER = "LastDataVersion";

    /** Memory stats stub. */
    private static final MemoryUsageMetrics MEM_STAT_STUB = new MemoryUsageMetrics();
    
    /** */
    private static final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

    /** */
    private static final RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();

    /** */
    private static final ThreadMXBean threads = ManagementFactory.getThreadMXBean();

    /** */
    private static final Collection<GarbageCollectorMXBean> gc = ManagementFactory.getGarbageCollectorMXBeans();

    /** JVM interface to memory consumption info */
    private static final MemoryMXBean mem = ManagementFactory.getMemoryMXBean();

    /** Metrics update worker. */
    private GridTimeoutProcessor.CancelableTask metricsUpdateTask;

    /** Available processors. */
    private final int availableProcessors = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public SystemMetricSource(GridKernalContext ctx) {
        super(SYS_METRICS, ctx);
    }

    /** {@inheritDoc} */
    @Override protected void init(MetricRegistryBuilder bldr, Holder hldr) {
        hldr.heap = new MemoryUsageMetrics(metricName("memory", "heap"), bldr);
        hldr.nonHeap = new MemoryUsageMetrics(metricName("memory", "nonheap"), bldr);

        hldr.heap.update(mem.getHeapMemoryUsage());
        hldr.nonHeap.update(mem.getNonHeapMemoryUsage());

        hldr.gcCpuLoad = bldr.doubleMetric(GC_CPU_LOAD, GC_CPU_LOAD_DESCRIPTION);

        hldr.cpuLoad = bldr.doubleMetric(CPU_LOAD, CPU_LOAD_DESCRIPTION);

        bldr.register("SystemLoadAverage", os::getSystemLoadAverage, Double.class, null);

        hldr.upTime = bldr.register(UP_TIME, rt::getUptime, null);

        hldr.threadCnt = bldr.register(THREAD_CNT, threads::getThreadCount, null);

        hldr.peakThreadCnt = bldr.register(PEAK_THREAD_CNT, threads::getPeakThreadCount, null);

        hldr.totalStartedThreadCnt = bldr.register(TOTAL_STARTED_THREAD_CNT, threads::getTotalStartedThreadCount, null);

        hldr.daemonThreadCnt = bldr.register(DAEMON_THREAD_CNT, threads::getDaemonThreadCount, null);

        bldr.register("CurrentThreadCpuTime", threads::getCurrentThreadCpuTime, null);

        bldr.register("CurrentThreadUserTime", threads::getCurrentThreadUserTime, null);

        bldr.register("availableProcessors", () -> availableProcessors, null);

        hldr.lastDataVer = bldr.longMetric(LAST_DATA_VER, "The latest data version on the node.");

        metricsUpdateTask = ctx().timeout().schedule(new MetricsUpdater(), METRICS_UPDATE_FREQ, METRICS_UPDATE_FREQ);
    }

    /** {@inheritDoc} */
    @Override protected void cleanup(Holder hldr) {
        //TODO: It should be done before metric source is disabled.
        U.closeQuiet(metricsUpdateTask);
    }

    /**
     * Returns the CPU usage usage in {@code [0, 1]} range.
     * The exact way how this number is calculated depends on SPI implementation.
     * <p>
     * If the CPU usage is not available, a negative value is returned.
     * <p>
     * This method is designed to provide a hint about the system load
     * and may be queried frequently. The load average may be unavailable on
     * some platform where it is expensive to implement this method.
     *
     * @return The estimated CPU usage in {@code [0, 1]} range. Negative value if not available.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public double cpuLoad() {
        Holder hldr = holder();

        return hldr != null ? hldr.cpuLoad.value() : Double.NaN;
    }

    /**
     * Returns average time spent in GC since the last update.
     *
     * @return Average time spent in GC since the last update.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public double gcCpuLoad() {
        Holder hldr = holder();

        return hldr != null ? hldr.gcCpuLoad.value() : Double.NaN;
    }

    /**
     * Returns memory usage metrics.
     *
     * @return Memory usage metrics.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public MemoryUsageMetrics heap() {
        Holder hldr = holder();

        return hldr != null ? hldr.heap : MEM_STAT_STUB;
    }

    /**
     * Returns offheap memory usage metrics.
     *
     * @return Offheap memory usage metrics.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public MemoryUsageMetrics nonHeap() {
        Holder hldr = holder();

        return hldr != null ? hldr.nonHeap : MEM_STAT_STUB;
    }

    /**
     * Returns the uptime of the JVM in milliseconds.
     *
     * @return Uptime of the JVM in milliseconds.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long upTime() {
        Holder hldr = holder();

        return hldr != null ? hldr.upTime.value() : -1;
    }

    /**
     * Returns the current number of live threads including both
     * daemon and non-daemon threads.
     *
     * @return Current number of live threads.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public int threadCount() {
        Holder hldr = holder();

        return hldr != null ? hldr.threadCnt.value() : -1;
    }

    /**
     * Returns the maximum live thread count since the JVM
     * started or peak was reset.
     * <p>
     * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
     * from the time of the node's startup.
     *
     * @return The peak live thread count.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public int peakThreadCount() {
        Holder hldr = holder();

        return hldr != null ? hldr.peakThreadCnt.value() : -1;
    }

    /**
     * Returns the total number of threads created and also started
     * since the JVM started.
     * <p>
     * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
     * from the time of the node's startup.
     *
     * @return The total number of threads started.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long totalStartedThreadCount() {
        Holder hldr = holder();

        return hldr != null ? hldr.totalStartedThreadCnt.value() : -1;
    }

    /**
     * Returns the current number of live daemon threads.
     *
     * @return Current number of live daemon threads.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public int daemonThreadCount() {
        Holder hldr = holder();

        return hldr != null ? hldr.daemonThreadCnt.value() : -1;
    }

    /**
     * Sets lates data version on the node.
     *
     * @param val Data version.
     * @deprecated Should be removed in Apache Ignite 3.0 because metric doesn't make sense.
     */
    @Deprecated
    public void lastDataVersion(long val) {
        Holder hldr = holder();

        if (hldr != null)
            hldr.lastDataVer.value(val);
    }

    /**
     * In-Memory Data Grid assigns incremental versions to all cache operations. This method provides
     * the latest data version on the node.
     *
     * @return Last data version.
     * @deprecated Should be removed in Apache Ignite 3.0 because metric doesn't make sense.
     */
    @Deprecated
    public long lastDataVersion() {
        Holder hldr = holder();

        return hldr != null ? hldr.lastDataVer.value() : -1;
    }

    //TODO: Consider moving to some utility package
    /**
     * Returns the current memory usage of the heap.
     * @return Memory usage or fake value with zero in case there was exception during take of metrics.
     */
    public static MemoryUsage heapMemoryUsage() {
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

    //TODO: Consider moving to some utility package
    /**
     * @return Memory usage of non-heap memory.
     */
    public static MemoryUsage nonHeapMemoryUsage() {
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

    /** Memory usage metrics. */
    public static class MemoryUsageMetrics {
        /** @see MemoryUsage#getInit() */
        private final AtomicLongMetric init;

        /** @see MemoryUsage#getUsed() */
        private final AtomicLongMetric used;

        /** @see MemoryUsage#getCommitted() */
        private final AtomicLongMetric committed;

        /** @see MemoryUsage#getMax() */
        private final AtomicLongMetric max;

        /**
         * @param metricNamePrefix Metric name prefix.
         * @param bldr Metric registry builder.
         */
        public MemoryUsageMetrics(String metricNamePrefix, MetricRegistryBuilder bldr) {
            init = bldr.longMetric(metricName(metricNamePrefix, "init"), null);
            used = bldr.longMetric(metricName(metricNamePrefix, "used"), null);
            committed = bldr.longMetric(metricName(metricNamePrefix, "committed"), null);
            max = bldr.longMetric(metricName(metricNamePrefix, "max"), null);
        }

        /** Default constructor. */
        private MemoryUsageMetrics() {
            init = new AtomicLongMetric("init", null);
            used = new AtomicLongMetric("used", null);
            committed = new AtomicLongMetric("committed", null);
            max = new AtomicLongMetric("max", null);
        }

        /** Updates metric to the provided values. */
        public void update(MemoryUsage usage) {
            init.value(usage.getInit());
            used.value(usage.getUsed());
            committed.value(usage.getCommitted());
            max.value(usage.getMax());
        }

        /**
         * Returns initial memory usage.
         *
         * @return Initial memory usage.
         */
        public long init() {
            return init.value();
        }

        /**
         * Returns used memory usage.
         *
         * @return Used memory usage.
         */
        public long used() {
            return used.value();
        }

        /**
         * Returns commited memory.
         *
         * @return Used commited memory.
         */
        public long committed() {
            return committed.value();
        }

        /**
         * Returns max memory usage.
         *
         * @return Max memory usage.
         */
        public long max() {
            return max.value();
        }
    }

    /** */
    private class MetricsUpdater implements Runnable {
        /** */
        private long prevGcTime = -1;

        /** */
        private long prevCpuTime = -1;

        /** {@inheritDoc} */
        @Override public void run() {
            Holder hldr = holder();

            if (hldr != null) {
                hldr.heap.update(heapMemoryUsage());
                hldr.nonHeap.update(nonHeapMemoryUsage());

                hldr.gcCpuLoad.value(getGcCpuLoad());
                hldr.cpuLoad.value(getCpuLoad());
            }
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
                cpuTime = U.<Long>property(os, "processCpuTime");
            }
            catch (IgniteException ignored) {
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

    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
    }

    /** */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        /** Heap memory metrics. */
        private MemoryUsageMetrics heap;

        /** Nonheap memory metrics. */
        private MemoryUsageMetrics nonHeap;

        /** GC CPU load. */
        private DoubleMetricImpl gcCpuLoad;

        /** CPU load. */
        private DoubleMetricImpl cpuLoad;

        /** */
        private LongGauge upTime;

        /** */
        private IntGauge threadCnt;

        /** */
        private IntGauge peakThreadCnt;

        /** */
        private LongGauge totalStartedThreadCnt;

        /** */
        private IntGauge daemonThreadCnt;

        /**
         * Last version metric.
         * @deprecated Useless metric. Should be removed for 3.0 release.
         */
        @Deprecated
        private AtomicLongMetric lastDataVer;
    }
}
