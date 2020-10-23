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
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
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
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.processors.metric.sources.StripedPoolMetricSource;
import org.apache.ignite.internal.processors.metric.sources.SystemMetricSource;
import org.apache.ignite.internal.processors.metric.sources.ThreadPoolExecutorMetricSource;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.ReadOnlyMetricManager;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_PHY_RAM;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.fromFullName;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.metric.sources.SystemMetricSource.SYS_METRICS;
import static org.apache.ignite.internal.util.IgniteUtils.notifyListeners;

/**
 * This manager should provide {@link ReadOnlyMetricManager} for each configured {@link MetricExporterSpi}.
 *
 * @see MetricExporterSpi
 * @see MetricRegistry
 */
//TODO: Remove ReadOnlyMetricRegistry interface
public class GridMetricManager extends GridManagerAdapter<MetricExporterSpi> implements ReadOnlyMetricManager {
    /** Class name for a SQL view metrics exporter. */
    public static final String SQL_SPI = "org.apache.ignite.internal.processors.metric.sql.SqlViewMetricExporterSpi";

    /** */
    private static final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

    //TODO: copy-on-write semantic on modification.
    //TODO: try to avoid find-semantic
    /** Registered metrics registries. */
    private final Map<String, ReadOnlyMetricRegistry> registries = new ConcurrentHashMap<>();

    /** Registered metric sources. */
    private final Map<String, MetricSource> sources = new ConcurrentHashMap<>();

    /** Prefix for {@link HitRateMetric} configuration property name. */
    public static final String HITRATE_CFG_PREFIX = metricName("metrics", "hitrate");

    /** Prefix for {@link HistogramMetric} configuration property name. */
    public static final String HISTOGRAM_CFG_PREFIX = metricName("metrics", "histogram");

    /** Metric registry creation listeners. */
    private final List<Consumer<ReadOnlyMetricRegistry>> metricRegCreationLsnrs = new CopyOnWriteArrayList<>();

    /** Metric registry remove listeners. */
    private final List<Consumer<ReadOnlyMetricRegistry>> metricRegRmvLsnrs = new CopyOnWriteArrayList<>();

    /** Read-only metastorage. */
    private volatile ReadableDistributedMetaStorage roMetastorage;

    /** Metastorage with the write access. */
    private volatile DistributedMetaStorage metastorage;

    /**
     * @param ctx Kernal context.
     */
    public GridMetricManager(GridKernalContext ctx) {
        super(ctx, ((Supplier<MetricExporterSpi[]>)() -> {
            MetricExporterSpi[] spi = ctx.config().getMetricExporterSpi();

            if (!IgniteComponentType.INDEXING.inClassPath())
                return spi;

            MetricExporterSpi[] spiWithSql = new MetricExporterSpi[spi != null ? spi.length + 1 : 1];

            if (!F.isEmpty(spi))
                System.arraycopy(spi, 0, spiWithSql, 0, spi.length);

            try {
                spiWithSql[spiWithSql.length - 1] = U.newInstance(SQL_SPI);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            return spiWithSql;
        }).get());

        //TODO: Why we do it here?
        ctx.addNodeAttribute(ATTR_PHY_RAM, totalSysMemory());
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        for (MetricExporterSpi spi : getSpis())
            spi.setMetricRegistry(this);

        startSpi();

        SystemMetricSource sysMetricsSrc = new SystemMetricSource(ctx);

        registerSource(sysMetricsSrc);
        enableMetrics(sysMetricsSrc);

        ctx.internalSubscriptionProcessor().registerDistributedMetastorageListener(
                new DistributedMetastorageLifecycleListener() {
                    /** {@inheritDoc} */
                    @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
                        roMetastorage = metastorage;

                        try {
                            metastorage.iterate(HITRATE_CFG_PREFIX, (name, val) -> onHitRateConfigChanged(
                                    name.substring(HITRATE_CFG_PREFIX.length() + 1), (Long) val));

                            metastorage.iterate(HISTOGRAM_CFG_PREFIX, (name, val) -> onHistogramConfigChanged(
                                    name.substring(HISTOGRAM_CFG_PREFIX.length() + 1), (long[]) val));
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }

                        metastorage.listen(n -> n.startsWith(HITRATE_CFG_PREFIX),
                                (name, oldVal, newVal) -> onHitRateConfigChanged(
                                        name.substring(HITRATE_CFG_PREFIX.length() + 1), (Long) newVal));

                        metastorage.listen(n -> n.startsWith(HISTOGRAM_CFG_PREFIX),
                                (name, oldVal, newVal) -> onHistogramConfigChanged(
                                        name.substring(HISTOGRAM_CFG_PREFIX.length() + 1), (long[]) newVal));
                    }

                    /** {@inheritDoc} */
                    @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
                        GridMetricManager.this.metastorage = metastorage;
                    }
                });
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        disableMetrics(SYS_METRICS);
        unregisterSource(SYS_METRICS);

        stopSpi();
    }

    //TODO: javadoc
    /**
     * @param name Name of metric source.
     */
    public void enableMetrics(String name) {
        MetricSource src = sources.get(name);

        assert src != null : "There is no registered metric source \"" + name + "\".";

        enableMetrics(src);
    }

    //TODO: javadoc
    /**
     * @param src Metric source.
     */
    public void enableMetrics(MetricSource src) {
        MetricRegistry reg = src.enable();

        if (reg != null)
            addRegistry(reg);
    }

    //TODO: javadoc
    /**
     * @param name Name of metric source.
     */
    public void disableMetrics(String name) {
        if (name.startsWith("sys"))
            System.out.println();

        MetricSource src = sources.get(name);

        assert src != null : "There is no registered metric source \"" + name + "\".";

        disableMetrics(src);
    }

    //TODO: javadoc
    /**
     * @param src Metric source.
     */
    public void disableMetrics(MetricSource src) {
        removeRegistry(src.name());

        src.disable();
    }

    /**
     * Adds metric registry.
     *
     * @param reg MetricRegistry
     * @throws IllegalStateException in case if provided metric registry is already registered.
     */
    //TODO: consider to narrow scope
    public void addRegistry(ReadOnlyMetricRegistry reg) {
        ReadOnlyMetricRegistry old = registries.put(reg.name(), reg);

        if (old != null) {
            throw new IllegalStateException("Metric registry with given name is already registered [name=" +
                    reg.name() + ']');
        }

        notifyListeners(reg, metricRegCreationLsnrs, log);

        // TODO: fix it. Below implementation form ignite-2.8
/*
        return registries.computeIfAbsent(name, n -> {
            MetricRegistry mreg = new MetricRegistry(name,
                    mname -> readFromMetastorage(metricName(HITRATE_CFG_PREFIX, mname)),
                    mname -> readFromMetastorage(metricName(HISTOGRAM_CFG_PREFIX, mname)),
                    log);

            notifyListeners(mreg, metricRegCreationLsnrs, log);

            return mreg;
        });
*/
    }

    /**
     * Removes metric registry with given name.
     *
     * @param name Metric registry name.
     */
    //TODO: consider to narrow scope
    public void removeRegistry(String name) {
        ReadOnlyMetricRegistry reg = registries.remove(name);

        if (reg != null)
            notifyListeners(reg, metricRegRmvLsnrs, log);
    }

    /**
     * Retirieves metric registry with given name.
     *
     * @param name Metric registry name.
     */
    //TODO: rename to registry()
    //TODO: consider to narrow scope
    public MetricRegistry getRegistry(String name) {
        return (MetricRegistry)registries.get(name);
    }

    /**
     * Returns metric scource with given name.
     *
     * @param name Metric source name.
     * @return Metric source.
     */
    public <T extends MetricSource> T source(String name) {
        return (T)sources.get(name);
    }

    /**
     * Registers given metric source.
     *
     * @param src Metric source.
     */
    public void registerSource(MetricSource src) {
        MetricSource old = sources.putIfAbsent(src.name(), src);

        if (old != null)
            throw new IllegalStateException("Metric source is already registered [name=" + src.name() + ']');
    }

    /**
     * Unregisters metric source with given name.
     *
     * @param name Metric source name.
     */
    public void unregisterSource(String name) {
        sources.remove(name);
    }

    /**
     * Unregisters metric source.
     *
     * @param src Metric source.
     */
    public void unregisterSource(MetricSource src) {
        sources.remove(src.name());
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
        metricRegRmvLsnrs.add(lsnr);
    }

    /**
     * Removes metric registry.
     *
     * @param regName Metric registry name.
     */
    //TODO: remove this MEthod. Use removeRegistry
    public void remove(String regName) {
        ReadOnlyMetricRegistry mreg = registries.remove(regName);

        if (mreg == null)
            return;

        notifyListeners(mreg, metricRegRmvLsnrs, log);

        DistributedMetaStorage metastorage0 = metastorage;

        if (metastorage0 == null)
            return;

        //TODO: fix it. Below implementation from ignite-2.8
/*
        try {
            GridCompoundFuture opsFut = new GridCompoundFuture<>();

            for (Metric m : mreg) {
                if (m instanceof HitRateMetric)
                    opsFut.add(metastorage0.removeAsync(metricName(HITRATE_CFG_PREFIX, m.name())));
                else if (m instanceof HistogramMetric)
                    opsFut.add(metastorage0.removeAsync(metricName(HISTOGRAM_CFG_PREFIX, m.name())));
            }

            opsFut.markInitialized();
            opsFut.get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
*/
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

        metastorage.write(metricName(HISTOGRAM_CFG_PREFIX, name), bounds);
    }

    /**
     * Change {@link HitRateMetric} instance configuration.
     *
     * @param name Metric name.
     * @param rateTimeInterval New rateTimeInterval.
     * @see HistogramMetricImpl#reset(long[])
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

        HistogramMetricImpl m = find(name, HistogramMetricImpl.class);

        if (m == null)
            return;

        m.reset(bounds);
    }

    /**
     * @param name Metric name.
     * @param type Metric type.
     * @return Metric.
     */
    private <T extends Metric> T find(String name, Class<T> type) {
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

        if (!m.getClass().isAssignableFrom(type)) {
            log.error("Metric '" + name + "' has wrong type[type=" + m.getClass().getSimpleName() + ']');

            return null;
        }

        return (T) m;
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
     * @param dataStreamExecSvc Data stream executor service.
     * @param restExecSvc Reset executor service.
     * @param affExecSvc Affinity executor service.
     * @param idxExecSvc Indexing executor service.
     * @param cbExecSvc Callback executor service.
     * @param qryExecSvc Query executor service.
     * @param schemaExecSvc Schema executor service.
     * @param rebalanceExecSvc Rebalance executor service.
     * @param rebalanceStripedExecSvc Rebalance striped executor service.
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
        StripedExecutor dataStreamExecSvc,
        ExecutorService restExecSvc,
        ExecutorService affExecSvc,
        @Nullable ExecutorService idxExecSvc,
        IgniteStripedThreadPoolExecutor cbExecSvc,
        ExecutorService qryExecSvc,
        ExecutorService schemaExecSvc,
        ExecutorService rebalanceExecSvc,
        IgniteStripedThreadPoolExecutor rebalanceStripedExecSvc,
        @Nullable final Map<String, ? extends ExecutorService> customExecSvcs
    ) {
        // Executors
        monitorExecutor("GridUtilityCacheExecutor", utilityCachePool);
        monitorExecutor("GridExecutionExecutor", execSvc);
        monitorExecutor("GridServicesExecutor", svcExecSvc);
        monitorExecutor("GridSystemExecutor", sysExecSvc);
        monitorExecutor("GridClassLoadingExecutor", p2pExecSvc);
        monitorExecutor("GridManagementExecutor", mgmtExecSvc);
        monitorExecutor("GridAffinityExecutor", affExecSvc);
        monitorExecutor("GridCallbackExecutor", cbExecSvc);
        monitorExecutor("GridQueryExecutor", qryExecSvc);
        monitorExecutor("GridSchemaExecutor", schemaExecSvc);
        monitorExecutor("GridRebalanceExecutor", rebalanceExecSvc);
        monitorExecutor("GridRebalanceStripedExecutor", rebalanceStripedExecSvc);

        monitorStripedPool("GridDataStreamExecutor", dataStreamExecSvc);

        if (idxExecSvc != null)
            monitorExecutor("GridIndexingExecutor", idxExecSvc);

        if (ctx.config().getConnectorConfiguration() != null)
            monitorExecutor("GridRestExecutor", restExecSvc);

        if (stripedExecSvc != null) {
            // Striped executor uses a custom adapter.
            monitorStripedPool("StripedExecutor", stripedExecSvc);
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
        ThreadPoolExecutorMetricSource src = new ThreadPoolExecutorMetricSource(name, ctx, execSvc);

        registerSource(src);

        enableMetrics(src);
    }

    /**
     * Creates a MetricSet for an stripped executor.
     *
     * @param name name of the bean to register
     * @param svc Executor.
     */
    private void monitorStripedPool(String name, StripedExecutor svc) {
        StripedPoolMetricSource src = new StripedPoolMetricSource(name, ctx, svc);

        registerSource(src);

        enableMetrics(src);
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
}
