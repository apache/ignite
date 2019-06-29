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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.processors.metric.impl.DoubleMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongMetricImpl;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_PHY_RAM;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * This manager should provide {@link MetricRegistry} for each configured {@link MetricExporterSpi}.
 *
 * @see MetricExporterSpi
 * @see MetricRegistry
 */
public class GridMetricManager extends GridManagerAdapter<MetricExporterSpi> {
    /** */
    public static final String ACTIVE_COUNT_DESC = "Approximate number of threads that are actively executing tasks.";

    /** */
    public static final String COMPLETED_TASK_DESC = "Approximate total number of tasks that have completed execution.";

    /** */
    public static final String CORE_SIZE_DESC = "The core number of threads.";

    /** */
    public static final String LARGEST_SIZE_DESC = "Largest number of threads that have ever simultaneously been in the pool.";

    /** */
    public static final String MAX_SIZE_DESC = "The maximum allowed number of threads.";

    /** */
    public static final String POOL_SIZE_DESC = "Current number of threads in the pool.";

    /** */
    public static final String TASK_COUNT_DESC = "Approximate total number of tasks that have been scheduled for execution.";

    /** */
    public static final String QUEUE_SIZE_DESC = "Current size of the execution queue.";

    /** */
    public static final String KEEP_ALIVE_TIME_DESC = "Thread keep-alive time, which is the amount of time which threads in excess of " +
        "the core pool size may remain idle before being terminated.";

    /** */
    public static final String IS_SHUTDOWN_DESC = "True if this executor has been shut down.";

    /** */
    public static final String IS_TERMINATED_DESC = "True if all tasks have completed following shut down.";

    /** */
    public static final String IS_TERMINATING_DESC = "True if terminating but not yet terminated.";

    /** */
    public static final String REJ_HND_DESC = "Class name of current rejection handler.";

    /** */
    public static final String THRD_FACTORY_DESC = "Class name of thread factory used to create new threads.";

    /** Group for a thread pools. */
    public static final String THREAD_POOLS = "threadPools";

    /** Metrics update frequency. */
    private static final long METRICS_UPDATE_FREQ = 3000;

    /** System metrics prefix. */
    public static final String SYS_METRICS = "sys";

    /** GC CPU load metric name. */
    public static final String GC_CPU_LOAD = "GcCpuLoad";

    /** CPU load metric name. */
    public static final String CPU_LOAD = "CpuLoad";

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

    /** Monitoring registry. */
    private MetricRegistry mreg;

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

    /**
     * @param ctx Kernal context.
     */
    public GridMetricManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getMetricExporterSpi());

        mreg = new MetricRegistryImpl(ctx.log(MetricRegistryImpl.class));

        ctx.addNodeAttribute(ATTR_PHY_RAM, totalSysMemory());

        heap = new MemoryUsageMetrics(metricName(SYS_METRICS, "memory", "heap"));
        nonHeap = new MemoryUsageMetrics(metricName(SYS_METRICS, "memory", "nonheap"));

        heap.update(mem.getHeapMemoryUsage());
        nonHeap.update(mem.getNonHeapMemoryUsage());

        MetricRegistry sysreg = mreg.withPrefix(SYS_METRICS);

        gcCpuLoad = sysreg.doubleMetric(GC_CPU_LOAD, "GC CPU load.");
        cpuLoad = sysreg.doubleMetric(CPU_LOAD, "CPU load.");

        sysreg.register("SystemLoadAverage", os::getSystemLoadAverage, Double.class, null);
        sysreg.register(UP_TIME, rt::getUptime, null);
        sysreg.register(THREAD_CNT, threads::getThreadCount, null);
        sysreg.register(PEAK_THREAD_CNT, threads::getPeakThreadCount, null);
        sysreg.register(TOTAL_STARTED_THREAD_CNT, threads::getTotalStartedThreadCount, null);
        sysreg.register(DAEMON_THREAD_CNT, threads::getDaemonThreadCount, null);
        sysreg.register("CurrentThreadCpuTime", threads::getCurrentThreadCpuTime, null);
        sysreg.register("CurrentThreadUserTime", threads::getCurrentThreadUserTime, null);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        metricsUpdateTask = ctx.timeout().schedule(new MetricsUpdater(), METRICS_UPDATE_FREQ, METRICS_UPDATE_FREQ);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        for (MetricExporterSpi spi : getSpis())
            spi.setMetricRegistry(mreg);

        startSpi();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();

        // Stop discovery worker and metrics updater.
        U.closeQuiet(metricsUpdateTask);
    }

    /**
     * @return Metric resitry.
     */
    public MetricRegistry registry() {
        return mreg;
    }

    /**
     * Registers all metrics for thread pools.
     *
     * @param utilityCachePool Utility cache pool.
     * @param execSvc Executor service.
     * @param svcExecSvc Services' executor service.
     * @param sysExecSvc System executor service.
     * @param stripedExecSvc Striped executor.
     * @param p2pExecSvc P2P executor service.
     * @param mgmtExecSvc Management executor service.
     * @param igfsExecSvc IGFS executor service.
     * @param dataStreamExecSvc Data stream executor service.
     * @param restExecSvc Reset executor service.
     * @param affExecSvc Affinity executor service.
     * @param idxExecSvc Indexing executor service.
     * @param callbackExecSvc Callback executor service.
     * @param qryExecSvc Query executor service.
     * @param schemaExecSvc Schema executor service.
     * @param customExecSvcs Custom named executors.
     */
    public void registerThreadPools(
        ExecutorService utilityCachePool,
        ExecutorService execSvc,
        ExecutorService svcExecSvc,
        ExecutorService sysExecSvc,
        StripedExecutor stripedExecSvc,
        ExecutorService p2pExecSvc,
        ExecutorService mgmtExecSvc,
        ExecutorService igfsExecSvc,
        StripedExecutor dataStreamExecSvc,
        ExecutorService restExecSvc,
        ExecutorService affExecSvc,
        @Nullable ExecutorService idxExecSvc,
        IgniteStripedThreadPoolExecutor callbackExecSvc,
        ExecutorService qryExecSvc,
        ExecutorService schemaExecSvc,
        @Nullable final Map<String, ? extends ExecutorService> customExecSvcs
    ) {
        // Executors
        monitorExecutor("GridUtilityCacheExecutor", utilityCachePool);
        monitorExecutor("GridExecutionExecutor", execSvc);
        monitorExecutor("GridServicesExecutor", svcExecSvc);
        monitorExecutor("GridSystemExecutor", sysExecSvc);
        monitorExecutor("GridClassLoadingExecutor", p2pExecSvc);
        monitorExecutor("GridManagementExecutor", mgmtExecSvc);
        monitorExecutor("GridIgfsExecutor", igfsExecSvc);
        monitorExecutor("GridDataStreamExecutor", dataStreamExecSvc);
        monitorExecutor("GridAffinityExecutor", affExecSvc);
        monitorExecutor("GridCallbackExecutor", callbackExecSvc);
        monitorExecutor("GridQueryExecutor", qryExecSvc);
        monitorExecutor("GridSchemaExecutor", schemaExecSvc);

        if (idxExecSvc != null)
            monitorExecutor("GridIndexingExecutor", idxExecSvc);

        if (ctx.config().getConnectorConfiguration() != null)
            monitorExecutor("GridRestExecutor", restExecSvc);

        if (stripedExecSvc != null) {
            // Striped executor uses a custom adapter.
            monitorStripedPool(stripedExecSvc);
        }

        if (customExecSvcs != null) {
            for (Map.Entry<String, ? extends ExecutorService> entry : customExecSvcs.entrySet())
                monitorExecutor(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Creates a MetricSet for an executor.
     *
     * @param name Name of the bean to register.
     * @param execSvc Executor to register a bean for.
     */
    private void monitorExecutor(String name, ExecutorService execSvc) {
        MetricRegistry mset = mreg.withPrefix(THREAD_POOLS, name);

        if (execSvc instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor exec = (ThreadPoolExecutor)execSvc;

            mset.register("ActiveCount", exec::getActiveCount, ACTIVE_COUNT_DESC);
            mset.register("CompletedTaskCount", exec::getCompletedTaskCount, COMPLETED_TASK_DESC);
            mset.register("CorePoolSize", exec::getCorePoolSize, CORE_SIZE_DESC);
            mset.register("LargestPoolSize", exec::getLargestPoolSize, LARGEST_SIZE_DESC);
            mset.register("MaximumPoolSize", exec::getMaximumPoolSize, MAX_SIZE_DESC);
            mset.register("PoolSize", exec::getPoolSize, POOL_SIZE_DESC);
            mset.register("TaskCount", exec::getTaskCount, TASK_COUNT_DESC);
            mset.register("QueueSize", () -> exec.getQueue().size(), QUEUE_SIZE_DESC);
            mset.register("KeepAliveTime", () -> exec.getKeepAliveTime(MILLISECONDS), KEEP_ALIVE_TIME_DESC);
            mset.register("Shutdown", exec::isShutdown, IS_SHUTDOWN_DESC);
            mset.register("Terminated", exec::isTerminated, IS_TERMINATED_DESC);
            mset.register("Terminating", exec::isTerminating, IS_TERMINATING_DESC);
            mset.register("RejectedExecutionHandlerClass", () -> {
                RejectedExecutionHandler hnd = exec.getRejectedExecutionHandler();

                return hnd == null ? "" : hnd.getClass().getName();
            }, String.class, REJ_HND_DESC);
            mset.register("ThreadFactoryClass", () -> {
                ThreadFactory factory = exec.getThreadFactory();

                return factory == null ? "" : factory.getClass().getName();
            }, String.class, THRD_FACTORY_DESC);
        }
        else {
            mset.metric("ActiveCount", ACTIVE_COUNT_DESC).value(0);
            mset.metric("CompletedTaskCount", COMPLETED_TASK_DESC).value(0);
            mset.metric("CorePoolSize", CORE_SIZE_DESC).value(0);
            mset.metric("LargestPoolSize", LARGEST_SIZE_DESC).value(0);
            mset.metric("MaximumPoolSize", MAX_SIZE_DESC).value(0);
            mset.metric("PoolSize", POOL_SIZE_DESC).value(0);
            mset.metric("TaskCount", TASK_COUNT_DESC);
            mset.metric("QueueSize", QUEUE_SIZE_DESC).value(0);
            mset.metric("KeepAliveTime", KEEP_ALIVE_TIME_DESC).value(0);
            mset.register("Shutdown", execSvc::isShutdown, IS_SHUTDOWN_DESC);
            mset.register("Terminated", execSvc::isTerminated, IS_TERMINATED_DESC);
            mset.metric("Terminating", IS_TERMINATING_DESC);
            mset.objectMetric("RejectedExecutionHandlerClass", String.class, REJ_HND_DESC).value("");
            mset.objectMetric("ThreadFactoryClass", String.class, THRD_FACTORY_DESC).value("");
        }
    }

    /**
     * Creates a MetricSet for an stripped executor.
     *
     * @param svc Executor.
     */
    private void monitorStripedPool(StripedExecutor svc) {
        MetricRegistry mset = mreg.withPrefix(THREAD_POOLS, "StripedExecutor");

        mset.register("DetectStarvation",
            svc::detectStarvation,
            "True if possible starvation in striped pool is detected.");

        mset.register("StripesCount",
            svc::stripes,
            "Stripes count.");

        mset.register("Shutdown",
            svc::isShutdown,
            "True if this executor has been shut down.");

        mset.register("Terminated",
            svc::isTerminated,
            "True if all tasks have completed following shut down.");

        mset.register("TotalQueueSize",
            svc::queueSize,
            "Total queue size of all stripes.");

        mset.register("TotalCompletedTasksCount",
            svc::completedTasks,
            "Completed tasks count of all stripes.");

        mset.register("StripesCompletedTasksCounts",
            svc::stripesCompletedTasks,
            long[].class,
            "Number of completed tasks per stripe.");

        mset.register("ActiveCount",
            svc::activeStripesCount,
            "Number of active tasks of all stripes.");

        mset.register("StripesActiveStatuses",
            svc::stripesActiveStatuses,
            boolean[].class,
            "Number of active tasks per stripe.");

        mset.register("StripesQueueSizes",
            svc::stripesQueueSizes,
            int[].class,
            "Size of queue per stripe.");
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

    /**
     * @return Total system memory.
     */
    private long totalSysMemory() {
        try {
            return U.<Long>property(os, "totalPhysicalMemorySize");
        }
        catch (RuntimeException ignored) {
            return -1;
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

    /** Memory usage metrics. */
    public class MemoryUsageMetrics {
        /** @see MemoryUsage#getInit() */
        private final LongMetricImpl init;

        /** @see MemoryUsage#getUsed() */
        private final LongMetricImpl used;

        /** @see MemoryUsage#getCommitted() */
        private final LongMetricImpl committed;

        /** @see MemoryUsage#getMax() */
        private final LongMetricImpl max;

        /**
         * @param prefix Metric prefix.
         */
        public MemoryUsageMetrics(String prefix) {
            MetricRegistry mreg = GridMetricManager.this.mreg.withPrefix(prefix);

            this.init = mreg.metric("init", null);
            this.used = mreg.metric("used", null);
            this.committed = mreg.metric("committed", null);
            this.max = mreg.metric("max", null);
        }

        /** Updates metric to the provided values. */
        public void update(MemoryUsage usage) {
            init.value(usage.getInit());
            used.value(usage.getUsed());
            committed.value(usage.getCommitted());
            max.value(usage.getMax());
        }
    }
}
