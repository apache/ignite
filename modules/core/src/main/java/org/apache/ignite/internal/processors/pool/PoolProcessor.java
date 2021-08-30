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

package org.apache.ignite.internal.processors.pool;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.ExecutorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.security.thread.SecurityAwareIoPool;
import org.apache.ignite.internal.processors.security.thread.SecurityAwareStripedExecutor;
import org.apache.ignite.internal.processors.security.thread.SecurityAwareStripedThreadPoolExecutor;
import org.apache.ignite.internal.processors.security.thread.SecurityAwareThreadPoolExecutor;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorkerListener;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.IoPool;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;

/**
 * Processor which abstracts out thread pool management.
 */
public class PoolProcessor extends GridProcessorAdapter {
    /** Executor service. */
    @GridToStringExclude
    private ThreadPoolExecutor execSvc;

    /** Executor service for services. */
    @GridToStringExclude
    private ThreadPoolExecutor svcExecSvc;

    /** System executor service. */
    @GridToStringExclude
    private ThreadPoolExecutor sysExecSvc;

    /** */
    @GridToStringExclude
    private StripedExecutor stripedExecSvc;

    /** Management executor service. */
    @GridToStringExclude
    private ThreadPoolExecutor mgmtExecSvc;

    /** P2P executor service. */
    @GridToStringExclude
    private ThreadPoolExecutor p2pExecSvc;

    /** Data streamer executor service. */
    @GridToStringExclude
    private StripedExecutor dataStreamerExecSvc;

    /** REST requests executor service. */
    @GridToStringExclude
    private ThreadPoolExecutor restExecSvc;

    /** Utility cache executor service. */
    private ThreadPoolExecutor utilityCacheExecSvc;

    /** Affinity executor service. */
    @GridToStringExclude
    private ThreadPoolExecutor affExecSvc;

    /** Indexing pool. */
    @GridToStringExclude
    private ThreadPoolExecutor idxExecSvc;

    /** Thread pool for create/rebuild indexes. */
    @GridToStringExclude
    private ThreadPoolExecutor buildIdxExecSvc;

    /** Continuous query executor service. */
    @GridToStringExclude
    private IgniteStripedThreadPoolExecutor callbackExecSvc;

    /** Query executor service. */
    @GridToStringExclude
    private ThreadPoolExecutor qryExecSvc;

    /** Query executor service. */
    @GridToStringExclude
    private ThreadPoolExecutor schemaExecSvc;

    /** Rebalance executor service. */
    @GridToStringExclude
    private ThreadPoolExecutor rebalanceExecSvc;

    /** Rebalance striped executor service. */
    @GridToStringExclude
    private IgniteStripedThreadPoolExecutor rebalanceStripedExecSvc;

    /** Map of {@link IoPool}-s injected by Ignite plugins. */
    private final IoPool[] extPools = new IoPool[128];

    /** Custom named pools. */
    private Map<String, ThreadPoolExecutor> customExecs;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public PoolProcessor(GridKernalContext ctx) {
        super(ctx);

        IgnitePluginProcessor plugins = ctx.plugins();

        if (plugins != null) {
            // Process custom IO messaging pool extensions:
            final IoPool[] executorExtensions = ctx.plugins().extensions(IoPool.class);

            if (executorExtensions != null) {
                // Store it into the map and check for duplicates:
                for (IoPool ex : executorExtensions) {
                    final byte id = ex.id();

                    // 1. Check the pool id is non-negative:
                    if (id < 0)
                        throw new IgniteException("Failed to register IO executor pool because its ID is " +
                            "negative: " + id);

                    // 2. Check the pool id is in allowed range:
                    if (GridIoPolicy.isReservedGridIoPolicy(id))
                        throw new IgniteException("Failed to register IO executor pool because its ID in in the " +
                            "reserved range: " + id);

                    // 3. Check the pool for duplicates:
                    if (extPools[id] != null)
                        throw new IgniteException("Failed to register IO executor pool because its ID as " +
                            "already used: " + id);

                    extPools[id] = ctx.security().enabled() ? new SecurityAwareIoPool(ctx.security(), ex) : ex;
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        IgniteConfiguration cfg = ctx.config();

        UncaughtExceptionHandler oomeHnd = ctx.uncaughtExceptionHandler();

        UncaughtExceptionHandler excHnd = new UncaughtExceptionHandler() {
            @Override public void uncaughtException(Thread t, Throwable e) {
                ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
            }
        };

        validateThreadPoolSize(cfg.getPublicThreadPoolSize(), "public");

        execSvc = createExecutorService(
            "pub",
            cfg.getIgniteInstanceName(),
            cfg.getPublicThreadPoolSize(),
            cfg.getPublicThreadPoolSize(),
            DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.PUBLIC_POOL,
            oomeHnd);

        execSvc.allowCoreThreadTimeOut(true);

        validateThreadPoolSize(cfg.getServiceThreadPoolSize(), "service");

        svcExecSvc = createExecutorService(
            "svc",
            cfg.getIgniteInstanceName(),
            cfg.getServiceThreadPoolSize(),
            cfg.getServiceThreadPoolSize(),
            DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.SERVICE_POOL,
            oomeHnd);

        svcExecSvc.allowCoreThreadTimeOut(true);

        validateThreadPoolSize(cfg.getSystemThreadPoolSize(), "system");

        sysExecSvc = createExecutorService(
            "sys",
            cfg.getIgniteInstanceName(),
            cfg.getSystemThreadPoolSize(),
            cfg.getSystemThreadPoolSize(),
            DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.SYSTEM_POOL,
            oomeHnd);

        sysExecSvc.allowCoreThreadTimeOut(true);

        validateThreadPoolSize(cfg.getStripedPoolSize(), "stripedPool");

        WorkersRegistry workerRegistry = ctx.workersRegistry();

        stripedExecSvc = createStripedExecutor(
            cfg.getStripedPoolSize(),
            cfg.getIgniteInstanceName(),
            "sys",
            log,
            new IgniteInClosure<Throwable>() {
                @Override public void apply(Throwable t) {
                    ctx.failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, t));
                }
            },
            false,
            workerRegistry,
            cfg.getFailureDetectionTimeout());

        // Note that since we use 'LinkedBlockingQueue', number of
        // maximum threads has no effect.
        // Note, that we do not pre-start threads here as management pool may
        // not be needed.
        validateThreadPoolSize(cfg.getManagementThreadPoolSize(), "management");

        mgmtExecSvc = createExecutorService(
            "mgmt",
            cfg.getIgniteInstanceName(),
            cfg.getManagementThreadPoolSize(),
            cfg.getManagementThreadPoolSize(),
            DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.MANAGEMENT_POOL,
            oomeHnd);

        mgmtExecSvc.allowCoreThreadTimeOut(true);

        // Note that since we use 'LinkedBlockingQueue', number of
        // maximum threads has no effect.
        // Note, that we do not pre-start threads here as class loading pool may
        // not be needed.
        validateThreadPoolSize(cfg.getPeerClassLoadingThreadPoolSize(), "peer class loading");

        p2pExecSvc = createExecutorService(
            "p2p",
            cfg.getIgniteInstanceName(),
            cfg.getPeerClassLoadingThreadPoolSize(),
            cfg.getPeerClassLoadingThreadPoolSize(),
            DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.P2P_POOL,
            oomeHnd);

        p2pExecSvc.allowCoreThreadTimeOut(true);

        dataStreamerExecSvc = createStripedExecutor(
            cfg.getDataStreamerThreadPoolSize(),
            cfg.getIgniteInstanceName(),
            "data-streamer",
            log,
            new IgniteInClosure<Throwable>() {
                @Override public void apply(Throwable t) {
                    ctx.failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, t));
                }
            },
            true,
            workerRegistry,
            cfg.getFailureDetectionTimeout());

        // Note that we do not pre-start threads here as this pool may not be needed.
        validateThreadPoolSize(cfg.getAsyncCallbackPoolSize(), "async callback");

        callbackExecSvc = new IgniteStripedThreadPoolExecutor(
            cfg.getAsyncCallbackPoolSize(),
            cfg.getIgniteInstanceName(),
            "callback",
            oomeHnd,
            false,
            0);

        if (cfg.getConnectorConfiguration() != null) {
            validateThreadPoolSize(cfg.getConnectorConfiguration().getThreadPoolSize(), "connector");

            restExecSvc = createExecutorService(
                "rest",
                cfg.getIgniteInstanceName(),
                cfg.getConnectorConfiguration().getThreadPoolSize(),
                cfg.getConnectorConfiguration().getThreadPoolSize(),
                DFLT_THREAD_KEEP_ALIVE_TIME,
                new LinkedBlockingQueue<>(),
                GridIoPolicy.UNDEFINED,
                oomeHnd
            );

            restExecSvc.allowCoreThreadTimeOut(true);
        }

        validateThreadPoolSize(cfg.getUtilityCacheThreadPoolSize(), "utility cache");

        utilityCacheExecSvc = createExecutorService(
            "utility",
            cfg.getIgniteInstanceName(),
            cfg.getUtilityCacheThreadPoolSize(),
            cfg.getUtilityCacheThreadPoolSize(),
            cfg.getUtilityCacheKeepAliveTime(),
            new LinkedBlockingQueue<>(),
            GridIoPolicy.UTILITY_CACHE_POOL,
            oomeHnd);

        utilityCacheExecSvc.allowCoreThreadTimeOut(true);

        affExecSvc = createExecutorService(
            "aff",
            cfg.getIgniteInstanceName(),
            1,
            1,
            DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.AFFINITY_POOL,
            oomeHnd);

        affExecSvc.allowCoreThreadTimeOut(true);

        if (IgniteComponentType.INDEXING.inClassPath()) {
            int cpus = Runtime.getRuntime().availableProcessors();

            idxExecSvc = createExecutorService(
                "idx",
                cfg.getIgniteInstanceName(),
                cpus,
                cpus * 2,
                3000L,
                new LinkedBlockingQueue<>(1000),
                GridIoPolicy.IDX_POOL,
                oomeHnd
            );

            int buildIdxThreadPoolSize = cfg.getBuildIndexThreadPoolSize();

            validateThreadPoolSize(buildIdxThreadPoolSize, "build-idx");

            buildIdxExecSvc = createExecutorService(
                "build-idx-runner",
                cfg.getIgniteInstanceName(),
                buildIdxThreadPoolSize,
                buildIdxThreadPoolSize,
                DFLT_THREAD_KEEP_ALIVE_TIME,
                new LinkedBlockingQueue<>(),
                GridIoPolicy.UNDEFINED,
                oomeHnd
            );

            buildIdxExecSvc.allowCoreThreadTimeOut(true);
        }

        validateThreadPoolSize(cfg.getQueryThreadPoolSize(), "query");

        qryExecSvc = createExecutorService(
            "query",
            cfg.getIgniteInstanceName(),
            cfg.getQueryThreadPoolSize(),
            cfg.getQueryThreadPoolSize(),
            DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.QUERY_POOL,
            oomeHnd);

        qryExecSvc.allowCoreThreadTimeOut(true);

        schemaExecSvc = createExecutorService(
            "schema",
            cfg.getIgniteInstanceName(),
            2,
            2,
            DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.SCHEMA_POOL,
            oomeHnd);

        schemaExecSvc.allowCoreThreadTimeOut(true);

        validateThreadPoolSize(cfg.getRebalanceThreadPoolSize(), "rebalance");

        rebalanceExecSvc = createExecutorService(
            "rebalance",
            cfg.getIgniteInstanceName(),
            cfg.getRebalanceThreadPoolSize(),
            cfg.getRebalanceThreadPoolSize(),
            DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.UNDEFINED,
            excHnd);

        rebalanceExecSvc.allowCoreThreadTimeOut(true);

        rebalanceStripedExecSvc = createStripedThreadPoolExecutor(
            cfg.getRebalanceThreadPoolSize(),
            cfg.getIgniteInstanceName(),
            "rebalance-striped",
            excHnd,
            true,
            DFLT_THREAD_KEEP_ALIVE_TIME);

        if (!F.isEmpty(cfg.getExecutorConfiguration())) {
            validateCustomExecutorsConfiguration(cfg.getExecutorConfiguration());

            customExecs = new HashMap<>();

            for (ExecutorConfiguration execCfg : cfg.getExecutorConfiguration()) {
                ThreadPoolExecutor exec = createExecutorService(
                    execCfg.getName(),
                    cfg.getIgniteInstanceName(),
                    execCfg.getSize(),
                    execCfg.getSize(),
                    DFLT_THREAD_KEEP_ALIVE_TIME,
                    new LinkedBlockingQueue<>(),
                    GridIoPolicy.UNDEFINED,
                    oomeHnd);

                customExecs.put(execCfg.getName(), exec);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        // Avoid external thread pools GC retention.
        Arrays.fill(extPools, null);

        stopExecutors(log);
    }

    /**
     * Get executor service for policy.
     *
     * @param plc Policy.
     * @return Executor service.
     * @throws IgniteCheckedException If failed.
     */
    public Executor poolForPolicy(byte plc) throws IgniteCheckedException {
        switch (plc) {
            case GridIoPolicy.P2P_POOL:
                return getPeerClassLoadingExecutorService();
            case GridIoPolicy.SYSTEM_POOL:
                return getSystemExecutorService();
            case GridIoPolicy.PUBLIC_POOL:
                return getExecutorService();
            case GridIoPolicy.MANAGEMENT_POOL:
                return getManagementExecutorService();
            case GridIoPolicy.AFFINITY_POOL:
                return getAffinityExecutorService();

            case GridIoPolicy.IDX_POOL:
                assert getIndexingExecutorService() != null : "Indexing pool is not configured.";

                return getIndexingExecutorService();

            case GridIoPolicy.UTILITY_CACHE_POOL:
                assert utilityCachePool() != null : "Utility cache pool is not configured.";

                return utilityCachePool();

            case GridIoPolicy.SERVICE_POOL:
                assert getServiceExecutorService() != null : "Service pool is not configured.";

                return getServiceExecutorService();

            case GridIoPolicy.DATA_STREAMER_POOL:
                assert getDataStreamerExecutorService() != null : "Data streamer pool is not configured.";

                return getDataStreamerExecutorService();

            case GridIoPolicy.QUERY_POOL:
                assert getQueryExecutorService() != null : "Query pool is not configured.";

                return getQueryExecutorService();

            case GridIoPolicy.SCHEMA_POOL:
                assert getSchemaExecutorService() != null : "Query pool is not configured.";

                return getSchemaExecutorService();

            default: {
                if (plc < 0)
                    throw new IgniteCheckedException("Policy cannot be negative: " + plc);

                if (GridIoPolicy.isReservedGridIoPolicy(plc))
                    throw new IgniteCheckedException("Policy is reserved for internal usage (range 0-31): " + plc);

                IoPool pool = extPools[plc];

                if (pool == null)
                    throw new IgniteCheckedException("No pool is registered for policy: " + plc);

                assert plc == pool.id();

                Executor res = pool.executor();

                if (res == null)
                    throw new IgniteCheckedException("Thread pool for policy is null: " + plc);

                return res;
            }
        }
    }

    /**
     * Gets executor service for custom policy by executor name.
     *
     * @param name Executor name.
     * @return Executor service.
     */
    @Nullable public Executor customExecutor(String name) {
        assert name != null;

        Executor exec = null;

        if (customExecs != null)
            exec = customExecs.get(name);

        return exec;
    }

    /**
     * Gets utility cache pool.
     *
     * @return Utility cache pool.
     */
    public ExecutorService utilityCachePool() {
        return utilityCacheExecSvc;
    }

    /**
     * Gets async callback pool.
     *
     * @return Async callback pool.
     */
    public IgniteStripedThreadPoolExecutor asyncCallbackPool() {
        return callbackExecSvc;
    }

    /**
     * @return Thread pool implementation to be used in grid to process job execution
     *      requests and user messages sent to the node.
     */
    public ExecutorService getExecutorService() {
        return execSvc;
    }

    /**
     * Executor service that is in charge of processing service proxy invocations.
     *
     * @return Thread pool implementation to be used in grid for service proxy invocations.
     */
    public ExecutorService getServiceExecutorService() {
        return svcExecSvc;
    }

    /**
     * Executor service that is in charge of processing internal system messages.
     *
     * @return Thread pool implementation to be used in grid for internal system messages.
     */
    public ExecutorService getSystemExecutorService() {
        return sysExecSvc;
    }

    /**
     * Executor service that is in charge of processing internal system messages
     * in stripes (dedicated threads).
     *
     * @return Thread pool implementation to be used in grid for internal system messages.
     */
    public StripedExecutor getStripedExecutorService() {
        return stripedExecSvc;
    }

    /**
     * Executor service that is in charge of processing internal and Visor
     * {@link org.apache.ignite.compute.ComputeJob GridJobs}.
     *
     * @return Thread pool implementation to be used in grid for internal and Visor
     *      jobs processing.
     */
    public ExecutorService getManagementExecutorService() {
        return mgmtExecSvc;
    }

    /**
     * @return Thread pool implementation to be used for peer class loading
     *      requests handling.
     */
    public ExecutorService getPeerClassLoadingExecutorService() {
        return p2pExecSvc;
    }

    /**
     * Executor service that is in charge of processing data stream messages.
     *
     * @return Thread pool implementation to be used for data stream messages.
     */
    public StripedExecutor getDataStreamerExecutorService() {
        return dataStreamerExecSvc;
    }

    /**
     * Should return an instance of fully configured thread pool to be used for
     * processing of client messages (REST requests).
     *
     * @return Thread pool implementation to be used for processing of client
     *      messages.
     */
    public ExecutorService getRestExecutorService() {
        return restExecSvc;
    }

    /**
     * Get affinity executor service.
     *
     * @return Affinity executor service.
     */
    public ExecutorService getAffinityExecutorService() {
        return affExecSvc;
    }

    /**
     * Get indexing executor service.
     *
     * @return Indexing executor service.
     */
    @Nullable public ExecutorService getIndexingExecutorService() {
        return idxExecSvc;
    }

    /**
     * Executor service that is in charge of processing query messages.
     *
     * @return Thread pool implementation to be used in grid for query messages.
     */
    public ExecutorService getQueryExecutorService() {
        return qryExecSvc;
    }

    /**
     * Executor services that is in charge of processing user compute task.
     *
     * @return Map of custom thread pool executors.
     */
    @Nullable public Map<String, ? extends ExecutorService> customExecutors() {
        return customExecs == null ? null : Collections.unmodifiableMap(customExecs);
    }

    /**
     * Executor service that is in charge of processing schema change messages.
     *
     * @return Executor service that is in charge of processing schema change messages.
     */
    public ExecutorService getSchemaExecutorService() {
        return schemaExecSvc;
    }

    /**
     * Executor service that is in charge of processing rebalance messages.
     *
     * @return Executor service that is in charge of processing rebalance messages.
     */
    public ExecutorService getRebalanceExecutorService() {
        return rebalanceExecSvc;
    }

    /**
     * Executor service that is in charge of processing unorderable rebalance messages.
     *
     * @return Executor service that is in charge of processing unorderable rebalance messages.
     */
    public IgniteStripedThreadPoolExecutor getStripedRebalanceExecutorService() {
        return rebalanceStripedExecSvc;
    }

    /**
     * Return Thread pool for create/rebuild indexes.
     *
     * @return Thread pool for create/rebuild indexes.
     */
    public ExecutorService buildIndexExecutorService() {
        return buildIdxExecSvc;
    }

    /**
     * Stops executor services if they has been started.
     *
     * @param log Grid logger.
     */
    private void stopExecutors(IgniteLogger log) {
        boolean interrupted = Thread.interrupted();

        try {
            stopExecutors0(log);
        }
        finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * Stops executor services if they has been started.
     *
     * @param log Grid logger.
     */
    private void stopExecutors0(IgniteLogger log) {
        assert log != null;

        U.shutdownNow(getClass(), execSvc, log);

        execSvc = null;

        U.shutdownNow(getClass(), svcExecSvc, log);

        svcExecSvc = null;

        U.shutdownNow(getClass(), sysExecSvc, log);

        sysExecSvc = null;

        U.shutdownNow(getClass(), qryExecSvc, log);

        qryExecSvc = null;

        U.shutdownNow(getClass(), schemaExecSvc, log);

        schemaExecSvc = null;

        U.shutdownNow(getClass(), rebalanceExecSvc, log);

        rebalanceExecSvc = null;

        U.shutdownNow(getClass(), rebalanceStripedExecSvc, log);

        rebalanceStripedExecSvc = null;

        U.shutdownNow(getClass(), stripedExecSvc, log);

        stripedExecSvc = null;

        U.shutdownNow(getClass(), mgmtExecSvc, log);

        mgmtExecSvc = null;

        U.shutdownNow(getClass(), p2pExecSvc, log);

        p2pExecSvc = null;

        U.shutdownNow(getClass(), dataStreamerExecSvc, log);

        dataStreamerExecSvc = null;

        if (restExecSvc != null)
            U.shutdownNow(getClass(), restExecSvc, log);

        restExecSvc = null;

        U.shutdownNow(getClass(), utilityCacheExecSvc, log);

        utilityCacheExecSvc = null;

        U.shutdownNow(getClass(), affExecSvc, log);

        affExecSvc = null;

        U.shutdownNow(getClass(), idxExecSvc, log);

        idxExecSvc = null;

        U.shutdownNow(getClass(), buildIdxExecSvc, log);

        buildIdxExecSvc = null;

        U.shutdownNow(getClass(), callbackExecSvc, log);

        callbackExecSvc = null;

        if (!F.isEmpty(customExecs)) {
            for (ThreadPoolExecutor exec : customExecs.values())
                U.shutdownNow(getClass(), exec, log);

            customExecs = null;
        }
    }

    /**
     * @param poolSize an actual value in the configuration.
     * @param poolName a name of the pool like 'management'.
     * @throws IgniteCheckedException If the poolSize is wrong.
     */
    private static void validateThreadPoolSize(int poolSize, String poolName)
        throws IgniteCheckedException {
        if (poolSize <= 0) {
            throw new IgniteCheckedException("Invalid " + poolName + " thread pool size" +
                " (must be greater than 0), actual value: " + poolSize);
        }
    }

    /**
     * @param cfgs Array of the executors configurations.
     * @throws IgniteCheckedException If configuration is wrong.
     */
    private static void validateCustomExecutorsConfiguration(ExecutorConfiguration[] cfgs)
        throws IgniteCheckedException {
        if (cfgs == null)
            return;

        Set<String> names = new HashSet<>(cfgs.length);

        for (ExecutorConfiguration cfg : cfgs) {
            if (F.isEmpty(cfg.getName()))
                throw new IgniteCheckedException("Custom executor name cannot be null or empty.");

            if (!names.add(cfg.getName()))
                throw new IgniteCheckedException("Duplicate custom executor name: " + cfg.getName());

            if (cfg.getSize() <= 0)
                throw new IgniteCheckedException("Custom executor size must be positive [name=" + cfg.getName() +
                    ", size=" + cfg.getSize() + ']');
        }
    }

    /** Creates instance {@link IgniteStripedThreadPoolExecutor} with a notion of whether {@link IgniteSecurity} is enabled. */
    private IgniteStripedThreadPoolExecutor createStripedThreadPoolExecutor(
        int concurrentLvl,
        String igniteInstanceName,
        String threadNamePrefix,
        UncaughtExceptionHandler eHnd,
        boolean allowCoreThreadTimeOut,
        long keepAliveTime
    ) {
         return ctx.security().enabled()
             ? new SecurityAwareStripedThreadPoolExecutor(
                 ctx.security(),
                 concurrentLvl,
                 igniteInstanceName,
                 threadNamePrefix,
                 eHnd,
                 allowCoreThreadTimeOut,
                 keepAliveTime)
             : new IgniteStripedThreadPoolExecutor(
                 concurrentLvl,
                 igniteInstanceName,
                 threadNamePrefix,
                 eHnd,
                 allowCoreThreadTimeOut,
                 keepAliveTime);
    }

    /** Creates instance {@link StripedExecutor} with a notion of whether {@link IgniteSecurity} is enabled. */
    private StripedExecutor createStripedExecutor(
        int cnt,
        String igniteInstanceName,
        String poolName,
        final IgniteLogger log,
        IgniteInClosure<Throwable> errHnd,
        boolean stealTasks,
        GridWorkerListener gridWorkerLsnr,
        long failureDetectionTimeout
    ) {
        return ctx.security().enabled()
            ? new SecurityAwareStripedExecutor(
                ctx.security(),
                cnt,
                igniteInstanceName,
                poolName,
                log,
                errHnd,
                stealTasks,
                gridWorkerLsnr,
                failureDetectionTimeout)
            : new StripedExecutor(cnt, igniteInstanceName, poolName, log, errHnd, stealTasks, gridWorkerLsnr, failureDetectionTimeout);
    }

    /** Creates instance {@link IgniteThreadPoolExecutor} with a notion of whether {@link IgniteSecurity} is enabled. */
    private IgniteThreadPoolExecutor createExecutorService(
        String threadNamePrefix,
        String igniteInstanceName,
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        BlockingQueue<Runnable> workQ,
        byte plc,
        UncaughtExceptionHandler eHnd
    ) {
        return ctx.security().enabled()
            ? new SecurityAwareThreadPoolExecutor(
                ctx.security(),
                threadNamePrefix,
                igniteInstanceName,
                corePoolSize,
                maxPoolSize,
                keepAliveTime,
                workQ,
                plc,
                eHnd)
            : new IgniteThreadPoolExecutor(
                threadNamePrefix,
                igniteInstanceName,
                corePoolSize,
                maxPoolSize,
                keepAliveTime,
                workQ,
                plc,
                eHnd);
    }
}
