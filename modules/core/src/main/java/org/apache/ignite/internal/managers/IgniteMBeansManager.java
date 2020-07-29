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

package org.apache.ignite.internal.managers;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import javax.management.JMException;
import javax.management.ObjectName;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.ClusterLocalNodeMetricsMXBeanImpl;
import org.apache.ignite.internal.ClusterMetricsMXBeanImpl;
import org.apache.ignite.internal.ComputeMXBeanImpl;
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.QueryMXBeanImpl;
import org.apache.ignite.internal.ServiceMXBeanImpl;
import org.apache.ignite.internal.StripedExecutorMXBeanAdapter;
import org.apache.ignite.internal.ThreadPoolMXBeanAdapter;
import org.apache.ignite.internal.TransactionMetricsMxBeanImpl;
import org.apache.ignite.internal.TransactionsMXBeanImpl;
import org.apache.ignite.internal.managers.encryption.EncryptionMXBeanImpl;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMXBeanImpl;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMXBeanImpl;
import org.apache.ignite.internal.processors.cluster.BaselineAutoAdjustMXBeanImpl;
import org.apache.ignite.internal.processors.metric.MetricsMxBeanImpl;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.worker.FailureHandlingMxBeanImpl;
import org.apache.ignite.internal.worker.WorkersControlMXBeanImpl;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.mxbean.BaselineAutoAdjustMXBean;
import org.apache.ignite.mxbean.ClusterMetricsMXBean;
import org.apache.ignite.mxbean.ComputeMXBean;
import org.apache.ignite.mxbean.DataStorageMXBean;
import org.apache.ignite.mxbean.EncryptionMXBean;
import org.apache.ignite.mxbean.FailureHandlingMxBean;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.mxbean.MetricsMxBean;
import org.apache.ignite.mxbean.QueryMXBean;
import org.apache.ignite.mxbean.ServiceMXBean;
import org.apache.ignite.mxbean.SnapshotMXBean;
import org.apache.ignite.mxbean.StripedExecutorMXBean;
import org.apache.ignite.mxbean.ThreadPoolMXBean;
import org.apache.ignite.mxbean.TransactionMetricsMxBean;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.mxbean.WorkersControlMXBean;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

/**
 * Class that registers and unregisters MBeans for kernal.
 */
public class IgniteMBeansManager {
    /** Ignite kernal */
    private final IgniteKernal kernal;

    /** Ignite kernal context. */
    private final GridKernalContextImpl ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** MBean names stored to be unregistered later. */
    private final Set<ObjectName> mBeanNames = new HashSet<>();

    /**
     * @param kernal Grid kernal.
     */
    public IgniteMBeansManager(IgniteKernal kernal) {
        this.kernal = kernal;
        ctx = (GridKernalContextImpl)kernal.context();
        log = ctx.log(IgniteMBeansManager.class);
    }

    /**
     * Registers all kernal MBeans (for kernal, metrics, thread pools).
     *
     * @param utilityCachePool Utility cache pool.
     * @param execSvc Executor service.
     * @param svcExecSvc Services' executor service.
     * @param sysExecSvc System executor service.
     * @param stripedExecSvc Striped executor.
     * @param p2pExecSvc P2P executor service.
     * @param mgmtExecSvc Management executor service.
     * @param dataStreamExecSvc data stream executor service.
     * @param restExecSvc Reset executor service.
     * @param affExecSvc Affinity executor service.
     * @param idxExecSvc Indexing executor service.
     * @param callbackExecSvc Callback executor service.
     * @param qryExecSvc Query executor service.
     * @param schemaExecSvc Schema executor service.
     * @param rebalanceExecSvc Rebalance executor service.
     * @param rebalanceStripedExecSvc Rebalance striped executor service.
     * @param customExecSvcs Custom named executors.
     * @param workersRegistry Worker registry.
     * @throws IgniteCheckedException if fails to register any of the MBeans.
     */
    public void registerAllMBeans(
        ExecutorService utilityCachePool,
        final ExecutorService execSvc,
        final ExecutorService svcExecSvc,
        final ExecutorService sysExecSvc,
        final StripedExecutor stripedExecSvc,
        ExecutorService p2pExecSvc,
        ExecutorService mgmtExecSvc,
        StripedExecutor dataStreamExecSvc,
        ExecutorService restExecSvc,
        ExecutorService affExecSvc,
        @Nullable ExecutorService idxExecSvc,
        IgniteStripedThreadPoolExecutor callbackExecSvc,
        ExecutorService qryExecSvc,
        ExecutorService schemaExecSvc,
        ExecutorService rebalanceExecSvc,
        IgniteStripedThreadPoolExecutor rebalanceStripedExecSvc,
        @Nullable final Map<String, ? extends ExecutorService> customExecSvcs,
        WorkersRegistry workersRegistry
    ) throws IgniteCheckedException {
        if (U.IGNITE_MBEANS_DISABLED)
            return;

        // Kernal
        registerMBean("Kernal", IgniteKernal.class.getSimpleName(), kernal, IgniteMXBean.class);

        // Metrics
        ClusterMetricsMXBean locMetricsBean = new ClusterLocalNodeMetricsMXBeanImpl(ctx.discovery());
        registerMBean("Kernal", locMetricsBean.getClass().getSimpleName(), locMetricsBean, ClusterMetricsMXBean.class);
        ClusterMetricsMXBean metricsBean = new ClusterMetricsMXBeanImpl(kernal.cluster());
        registerMBean("Kernal", metricsBean.getClass().getSimpleName(), metricsBean, ClusterMetricsMXBean.class);

        // Transaction metrics
        TransactionMetricsMxBean txMetricsMXBean = new TransactionMetricsMxBeanImpl(ctx.cache().transactions().metrics());
        registerMBean("TransactionMetrics", txMetricsMXBean.getClass().getSimpleName(), txMetricsMXBean, TransactionMetricsMxBean.class);

        // Transactions
        TransactionsMXBean txMXBean = new TransactionsMXBeanImpl(ctx);
        registerMBean("Transactions", txMXBean.getClass().getSimpleName(), txMXBean, TransactionsMXBean.class);

        // Queries management
        QueryMXBean qryMXBean = new QueryMXBeanImpl(ctx);
        registerMBean("Query", qryMXBean.getClass().getSimpleName(), qryMXBean, QueryMXBean.class);

        // Compute task management
        ComputeMXBean computeMXBean = new ComputeMXBeanImpl(ctx);
        registerMBean("Compute", computeMXBean.getClass().getSimpleName(), computeMXBean, ComputeMXBean.class);

        // Service management
        ServiceMXBean serviceMXBean = new ServiceMXBeanImpl(ctx);
        registerMBean("Service", serviceMXBean.getClass().getSimpleName(), serviceMXBean, ServiceMXBean.class);

        // Data storage
        DataStorageMXBean dataStorageMXBean = new DataStorageMXBeanImpl(ctx);
        registerMBean("DataStorage", dataStorageMXBean.getClass().getSimpleName(), dataStorageMXBean, DataStorageMXBean.class);

        // Baseline configuration
        BaselineAutoAdjustMXBean baselineAutoAdjustMXBean = new BaselineAutoAdjustMXBeanImpl(ctx);
        registerMBean("Baseline", baselineAutoAdjustMXBean.getClass().getSimpleName(), baselineAutoAdjustMXBean,
            BaselineAutoAdjustMXBean.class);

        // Encryption
        EncryptionMXBean encryptionMXBean = new EncryptionMXBeanImpl(ctx);
        registerMBean("Encryption", encryptionMXBean.getClass().getSimpleName(), encryptionMXBean,
            EncryptionMXBean.class);

        // Snapshot.
        SnapshotMXBean snpMXBean = new SnapshotMXBeanImpl(ctx);
        registerMBean("Snapshot", snpMXBean.getClass().getSimpleName(), snpMXBean, SnapshotMXBean.class);

        // Metrics configuration
        MetricsMxBean metricsMxBean = new MetricsMxBeanImpl(ctx.metric(), log);
        registerMBean("Metrics", metricsMxBean.getClass().getSimpleName(), metricsMxBean, MetricsMxBean.class);

        // Executors
        registerExecutorMBean("GridUtilityCacheExecutor", utilityCachePool);
        registerExecutorMBean("GridExecutionExecutor", execSvc);
        registerExecutorMBean("GridServicesExecutor", svcExecSvc);
        registerExecutorMBean("GridSystemExecutor", sysExecSvc);
        registerExecutorMBean("GridClassLoadingExecutor", p2pExecSvc);
        registerExecutorMBean("GridManagementExecutor", mgmtExecSvc);
        registerExecutorMBean("GridAffinityExecutor", affExecSvc);
        registerExecutorMBean("GridCallbackExecutor", callbackExecSvc);
        registerExecutorMBean("GridQueryExecutor", qryExecSvc);
        registerExecutorMBean("GridSchemaExecutor", schemaExecSvc);
        registerExecutorMBean("GridRebalanceExecutor", rebalanceExecSvc);
        registerExecutorMBean("GridRebalanceStripedExecutor", rebalanceStripedExecSvc);

        registerStripedExecutorMBean("GridDataStreamExecutor", dataStreamExecSvc);

        if (idxExecSvc != null)
            registerExecutorMBean("GridIndexingExecutor", idxExecSvc);

        if (ctx.config().getConnectorConfiguration() != null)
            registerExecutorMBean("GridRestExecutor", restExecSvc);

        if (stripedExecSvc != null) {
            // striped executor uses a custom adapter
            registerStripedExecutorMBean("StripedExecutor", stripedExecSvc);
        }

        if (customExecSvcs != null) {
            for (Map.Entry<String, ? extends ExecutorService> entry : customExecSvcs.entrySet())
                registerExecutorMBean(entry.getKey(), entry.getValue());
        }

        if (U.IGNITE_TEST_FEATURES_ENABLED) {
            WorkersControlMXBean workerCtrlMXBean = new WorkersControlMXBeanImpl(workersRegistry);

            registerMBean("Kernal", workerCtrlMXBean.getClass().getSimpleName(),
                workerCtrlMXBean, WorkersControlMXBean.class);
        }

        FailureHandlingMxBean blockOpCtrlMXBean = new FailureHandlingMxBeanImpl(workersRegistry,
            ctx.cache().context().database());

        registerMBean("Kernal", blockOpCtrlMXBean.getClass().getSimpleName(), blockOpCtrlMXBean,
            FailureHandlingMxBean.class);

        if (ctx.query().moduleEnabled())
            ctx.query().getIndexing().registerMxBeans(this);
    }

    /**
     * Registers a {@link ThreadPoolMXBean} for an executor.
     *
     * @param name name of the bean to register
     * @param exec executor to register a bean for
     * @throws IgniteCheckedException if registration fails.
     */
    private void registerExecutorMBean(String name, ExecutorService exec) throws IgniteCheckedException {
        registerMBean("Thread Pools", name, new ThreadPoolMXBeanAdapter(exec), ThreadPoolMXBean.class);
    }

    /**
     * Registers a {@link StripedExecutorMXBean} for an striped executor.
     *
     * @param name name of the bean to register
     * @param exec executor to register a bean for
     * @throws IgniteCheckedException if registration fails.
     */
    private void registerStripedExecutorMBean(String name, StripedExecutor exec) throws IgniteCheckedException {
        registerMBean("Thread Pools", name, new StripedExecutorMXBeanAdapter(exec), StripedExecutorMXBean.class);
    }

    /**
     * Register an Ignite MBean.
     *
     * @param grp bean group name
     * @param name bean name
     * @param impl bean implementation
     * @param itf bean interface
     * @param <T> bean type
     * @throws IgniteCheckedException if registration fails
     */
    public <T> void registerMBean(String grp, String name, T impl, Class<T> itf) throws IgniteCheckedException {
        assert !U.IGNITE_MBEANS_DISABLED;

        try {
            ObjectName objName = U.registerMBean(
                ctx.config().getMBeanServer(),
                ctx.config().getIgniteInstanceName(),
                grp, name, impl, itf);

            if (log.isDebugEnabled())
                log.debug("Registered MBean: " + objName);

            mBeanNames.add(objName);
        }
        catch (JMException e) {
            throw new IgniteCheckedException("Failed to register MBean " + name, e);
        }
    }

    /**
     * Unregisters all previously registered MBeans.
     *
     * @return {@code true} if all mbeans were unregistered successfully; {@code false} otherwise.
     */
    public boolean unregisterAllMBeans() {
        boolean success = true;

        for (ObjectName name : mBeanNames)
            success = success && unregisterMBean(name);

        return success;
    }

    /**
     * Unregisters given MBean.
     *
     * @param mbean MBean to unregister.
     * @return {@code true} if successfully unregistered, {@code false} otherwise.
     */
    private boolean unregisterMBean(ObjectName mbean) {
        assert !U.IGNITE_MBEANS_DISABLED;

        try {
            ctx.config().getMBeanServer().unregisterMBean(mbean);

            if (log.isDebugEnabled())
                log.debug("Unregistered MBean: " + mbean);

            return true;
        }
        catch (JMException e) {
            U.error(log, "Failed to unregister MBean.", e);

            return false;
        }
    }
}
