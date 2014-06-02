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
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.cache.eviction.*;
import org.gridgain.grid.cache.eviction.fifo.*;
import org.gridgain.grid.cache.eviction.lru.*;
import org.gridgain.grid.cache.eviction.random.*;
import org.gridgain.grid.dr.cache.receiver.*;
import org.gridgain.grid.dr.cache.sender.*;
import org.gridgain.grid.dr.hub.receiver.*;
import org.gridgain.grid.dr.hub.sender.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.kernal.visor.cmd.dto.*;
import org.gridgain.grid.kernal.visor.cmd.dto.cache.*;
import org.gridgain.grid.kernal.visor.cmd.dto.node.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.lang.reflect.*;
import java.util.*;

import static java.lang.System.*;
import static org.gridgain.grid.GridSystemProperties.*;

/**
 * Grid configuration data collect task.
 */
@GridInternal
public class VisorConfigCollectorTask extends VisorOneNodeTask<VisorOneNodeArg, VisorGridConfig> {
    /**
     * Returns boolean value from system property or provided function.
     *
     * @param propName System property host.
     * @param dflt Function that returns `Boolean`.
     * @return `Boolean` value
     */
    private static boolean boolValue(String propName, boolean dflt) {
        String sysProp = getProperty(propName);

        return (sysProp != null && !sysProp.isEmpty()) ? Boolean.getBoolean(sysProp) : dflt;
    }

    /**
     * Returns boolean value from system property or provided function.
     *
     * @param propName System property host.
     * @param dflt Function that returns `Boolean`.
     * @return `Boolean` value
     */
    private static Integer intValue(String propName, Integer dflt) {
        String sysProp = getProperty(propName);

        return (sysProp != null && !sysProp.isEmpty()) ? Integer.getInteger(sysProp) : dflt;
    }

    /**
     * Returns compact class host.
     *
     * @param obj Object to compact.
     * @return String.
     */
    private static Object compactObject(Object obj) {
        if (obj == null)
            return null;

        if (obj instanceof Enum)
            return obj.toString();

        if (obj instanceof String || obj instanceof Boolean || obj instanceof Number)
            return obj;

        if (obj instanceof Collection) {
            Collection col = (Collection)obj;

            Object[] res = new Object[col.size()];

            int i = 0;

            for (Object elm : col) {
                res[i++] = compactObject(elm);
            }

            return res;
        }

        if (obj.getClass().isArray()) {
            Class<?> arrType = obj.getClass().getComponentType();

            if (arrType.isPrimitive()) {
                if (obj instanceof boolean[])
                    return Arrays.toString((boolean[])obj);
                if (obj instanceof byte[])
                    return Arrays.toString((byte[])obj);
                if (obj instanceof short[])
                    return Arrays.toString((short[])obj);
                if (obj instanceof int[])
                    return Arrays.toString((int[])obj);
                if (obj instanceof long[])
                    return Arrays.toString((long[])obj);
                if (obj instanceof float[])
                    return Arrays.toString((float[])obj);
                if (obj instanceof double[])
                    return Arrays.toString((double[])obj);
            }

            Object[] arr = (Object[])obj;

            int iMax = arr.length - 1;

            StringBuilder sb = new StringBuilder("[");

            for (int i = 0; i < 0; i++) {
                sb.append(compactObject(arr[i]));

                if (i != iMax)
                    sb.append(", ");
            }

            sb.append("]");

            return sb.toString();
        }

        return U.compact(obj.getClass().getName());
    }

    private static String compactClass(Object obj) {
        if (obj == null)
            return null;

        return U.compact(obj.getClass().getName());
    }

    /**
     * Joins array elements to string.
     *
     * @param arr Array.
     * @return String.
     */
    private static String compactArray(Object[] arr) {
        if (arr == null || arr.length == 0)
            return null;

        String sep = ", ";

        StringBuilder sb = new StringBuilder();

        for (Object s: arr)
            sb.append(s).append(sep);

        if (sb.length() > 0)
            sb.setLength(sb.length() - sep.length());

        return U.compact(sb.toString());
    }

    /**
     * Grid configuration data collect job.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorConfigurationJob extends VisorOneNodeJob<VisorOneNodeArg, VisorGridConfig> {
        /** */
        private static final long serialVersionUID = 0L;

        public VisorConfigurationJob(VisorOneNodeArg arg) {
            super(arg);
        }

        /**
         * Collects SPI information based on GridSpiConfiguration-annotated methods.
         * Methods with `Deprecated` annotation are skipped.
         *
         * @param spi SPI to collect information on.
         * @return Tuple where first component is SPI name and
         */
        private T2<String, Map<String, Object>> collectSpiInfo(GridSpi spi) {
            Class<? extends GridSpi> spiCls = spi.getClass();

            HashMap<String, Object> res = new HashMap<>();

            res.put("Class Name", compactClass(spi));

            for (Method mtd : spiCls.getDeclaredMethods()) {
                if (mtd.isAnnotationPresent(GridSpiConfiguration.class) && !mtd.isAnnotationPresent(Deprecated.class)) {
                    String mtdName = mtd.getName();

                    if (mtdName.startsWith("set")) {
                        String propName = Character.toLowerCase(mtdName.charAt(3)) + mtdName.substring(4);

                        String[] getterNames = new String[] {
                            "get" + mtdName.substring(3),
                            "is" + mtdName.substring(3),
                            "get" + mtdName.substring(3) + "Formatted"
                        };

                        try {
                            for (String getterName : getterNames) {
                                try {
                                    Method getter = spiCls.getDeclaredMethod(getterName);

                                    Object getRes = getter.invoke(spi);

                                    res.put(propName, compactObject(getRes));

                                    break;
                                }
                                catch (NoSuchMethodException ignored) {
                                }
                            }
                        }
                        catch (IllegalAccessException ignored) {
                            res.put(propName, "Error: Method Cannot Be Accessed");
                        }
                        catch (InvocationTargetException ite) {
                            res.put(propName, ("Error: Method Threw An Exception: " + ite));
                        }
                    }
                }
            }

            return new T2<String, Map<String, Object>>(spi.getName(), res);
        }

        private T2<String, Map<String, Object>>[] collectSpiInfo(GridSpi[] spis) {
            GridBiTuple[] res = new GridBiTuple[spis.length];

            for (int i = 0; i < spis.length; i++)
                res[i] = collectSpiInfo(spis[i]);

            return (T2<String, Map<String, Object>>[]) res;
        }

        @Override protected VisorGridConfig run(VisorOneNodeArg arg) {
            final GridConfiguration c = g.configuration();
            final GridProductLicense lic = g.product().license();

            final VisorBasicConfig basic = new VisorBasicConfig(
                c.getGridName(),
                getProperty(GG_HOME, c.getGridGainHome()),
                getProperty(GG_LOCAL_HOST, c.getLocalHost()),
                g.localNode().id(),
                compactClass(c.getMarshaller()),
                compactObject(c.getDeploymentMode()),
                boolValue(GG_DAEMON, c.isDaemon()),
                g.isJmxRemoteEnabled(),
                g.isRestartEnabled(),
                c.getNetworkTimeout(),
                c.getLicenseUrl(),
                compactClass(c.getGridLogger()),
                c.getDiscoveryStartupDelay(),
                compactClass(c.getMBeanServer()),
                boolValue(GG_NO_ASCII, false),
                boolValue(GG_NO_DISCO_ORDER, false),
                boolValue(GG_NO_SHUTDOWN_HOOK, false),
                getProperty(GG_PROG_NAME),
                boolValue(GG_QUIET, true),
                getProperty(GG_SUCCESS_FILE),
                boolValue(GG_UPDATE_NOTIFIER, true));

            final VisorMetricsConfig metrics = new VisorMetricsConfig(
                c.getMetricsExpireTime(),
                c.getMetricsHistorySize(),
                c.getMetricsLogFrequency());

            final VisorSpisConfig spis = new VisorSpisConfig(
                collectSpiInfo(c.getDiscoverySpi()),
                collectSpiInfo(c.getCommunicationSpi()),
                collectSpiInfo(c.getEventStorageSpi()),
                collectSpiInfo(c.getCollisionSpi()),
                collectSpiInfo(c.getAuthenticationSpi()),
                collectSpiInfo(c.getSecureSessionSpi()),
                collectSpiInfo(c.getDeploymentSpi()),
                collectSpiInfo(c.getCheckpointSpi()),
                collectSpiInfo(c.getFailoverSpi()),
                collectSpiInfo(c.getLoadBalancingSpi()),
                collectSpiInfo(c.getSwapSpaceSpi()),
                collectSpiInfo(c.getIndexingSpi())
            );

            final VisorPeerToPeerConfig p2p = new VisorPeerToPeerConfig(
                c.isPeerClassLoadingEnabled(),
                c.getPeerClassLoadingMissedResourcesCacheSize(),
                compactArray(c.getPeerClassLoadingLocalClassPathExclude())
            );

            final VisorEmailConfig email = new VisorEmailConfig(
                getProperty(GG_SMTP_HOST, c.getSmtpHost()),
                intValue(GG_SMTP_PORT, c.getSmtpPort()),
                getProperty(GG_SMTP_USERNAME, c.getSmtpUsername()),
                getProperty(GG_ADMIN_EMAILS, compactArray(c.getAdminEmails())),
                getProperty(GG_SMTP_FROM, c.getSmtpFromEmail()),
                boolValue(GG_SMTP_SSL, c.isSmtpSsl()),
                boolValue(GG_SMTP_STARTTLS, c.isSmtpStartTls()));

            final VisorLifecycleConfig lifecycle = new VisorLifecycleConfig(
                compactArray(c.getLifecycleBeans()),
                boolValue(GG_LIFECYCLE_EMAIL_NOTIFY,
                c.isLifeCycleEmailNotification())
            );

            final VisorExecServiceConfig execSvc = new VisorExecServiceConfig(
                compactClass(c.getExecutorService()),
                c.getExecutorServiceShutdown(),
                compactClass(c.getSystemExecutorService()),
                c.getSystemExecutorServiceShutdown(),
                compactClass(c.getPeerClassLoadingExecutorService()),
                c.getPeerClassLoadingExecutorServiceShutdown()
            );

            final VisorSegmentationConfig seg = new VisorSegmentationConfig(
                c.getSegmentationPolicy(),
                compactArray(c.getSegmentationResolvers()),
                c.getSegmentCheckFrequency(),
                c.isWaitForSegmentOnStart(),
                c.isAllSegmentationResolversPassRequired()
            );

            final VisorRestConfig rest = new VisorRestConfig(
                c.isRestEnabled(),
                c.isRestTcpSslEnabled(),
                c.getRestAccessibleFolders(),
                c.getRestJettyPath(),
                getProperty(GG_JETTY_HOST),
                intValue(GG_JETTY_PORT, null),
                c.getRestTcpHost(),
                c.getRestTcpPort(),
                compactClass(c.getRestTcpSslContextFactory())
            );

            final List<VisorCacheConfig> caches = new ArrayList<>(c.getCacheConfiguration().length);

            for (GridCacheConfiguration cacheCfg : c.getCacheConfiguration()) {
                GridCacheAffinityFunction affFunc = cacheCfg.getAffinity();

                Integer dfltReplicas = null;
                Boolean excludeNeighbors = null;

                if (affFunc instanceof GridCacheConsistentHashAffinityFunction) {
                    GridCacheConsistentHashAffinityFunction hashAffFunc = (GridCacheConsistentHashAffinityFunction)affFunc;

                    dfltReplicas = hashAffFunc.getDefaultReplicas();
                    excludeNeighbors = hashAffFunc.isExcludeNeighbors();
                }

                VisorAffinityConfig affinity = new VisorAffinityConfig(
                    compactClass(cacheCfg.getAffinity()),
                    compactClass(cacheCfg.getAffinityMapper()),
                    cacheCfg.getBackups(),
                    affFunc == null ? null : affFunc.partitions(),
                    dfltReplicas,
                    excludeNeighbors
                );

                VisorPreloadConfig preload = new VisorPreloadConfig(
                    cacheCfg.getPreloadMode(),
                    cacheCfg.getPreloadBatchSize(),
                    cacheCfg.getPreloadThreadPoolSize(),
                    cacheCfg.getPreloadPartitionedDelay(),
                    cacheCfg.getPreloadThrottle(),
                    cacheCfg.getPreloadTimeout()
                );

                Integer policyMaxSize = null;

                final GridCacheEvictionPolicy policy = cacheCfg.getEvictionPolicy();

                if (policy instanceof GridCacheLruEvictionPolicyMBean)
                    policyMaxSize = ((GridCacheLruEvictionPolicyMBean)policy).getMaxSize();
                else if (policy instanceof GridCacheRandomEvictionPolicyMBean)
                    policyMaxSize = ((GridCacheRandomEvictionPolicyMBean)policy).getMaxSize();
                else if (policy instanceof GridCacheFifoEvictionPolicyMBean)
                    policyMaxSize = ((GridCacheFifoEvictionPolicyMBean)policy).getMaxSize();

                VisorEvictionConfig evict = new VisorEvictionConfig(
                    compactClass(cacheCfg.getEvictionPolicy()),
                    policyMaxSize,
                    compactClass(cacheCfg.getEvictionFilter()),
                    cacheCfg.getEvictSynchronizedConcurrencyLevel(),
                    cacheCfg.getEvictSynchronizedTimeout(),
                    cacheCfg.getEvictSynchronizedKeyBufferSize(),
                    cacheCfg.isEvictSynchronized(),
                    cacheCfg.isEvictNearSynchronized(),
                    cacheCfg.getEvictMaxOverflowRatio()
                );

                VisorNearCacheConfig near = new VisorNearCacheConfig(
                    GridCacheUtils.isNearEnabled(cacheCfg),
                    cacheCfg.getNearStartSize(),
                    compactClass(cacheCfg.getNearEvictionPolicy())
                );

                VisorDefaultConfig dflt = new VisorDefaultConfig(
                    cacheCfg.getDefaultTxIsolation(),
                    cacheCfg.getDefaultTxConcurrency(),
                    cacheCfg.getDefaultTxTimeout(),
                    cacheCfg.getDefaultLockTimeout(),
                    cacheCfg.getDefaultQueryTimeout()
                );

                VisorDgcConfig dgc = new VisorDgcConfig(
                    cacheCfg.getDgcFrequency(),
                    cacheCfg.isDgcRemoveLocks(),
                    cacheCfg.getDgcSuspectLockTimeout()
                );

                VisorStoreConfig store = new VisorStoreConfig(
                    compactClass(cacheCfg.getStore()),
                    cacheCfg.isStoreValueBytes()
                );

                VisorWriteBehindConfig writeBehind = new VisorWriteBehindConfig(
                    cacheCfg.isWriteBehindEnabled(),
                    cacheCfg.getWriteBehindBatchSize(),
                    cacheCfg.getWriteBehindFlushFrequency(),
                    cacheCfg.getWriteBehindFlushSize(),
                    cacheCfg.getWriteBehindFlushThreadCount()
                );

                final GridDrSenderCacheConfiguration sender = cacheCfg.getDrSenderConfiguration();

                VisorDrSenderConfig drSenderCfg = null;

                if (sender != null)
                    drSenderCfg = new VisorDrSenderConfig(
                        sender.getMode(),
                        sender.getBatchSendSize(),
                        sender.getBatchSendFrequency(),
                        sender.getMaxBatches(),
                        sender.getSenderHubLoadBalancingMode(),
                        sender.getStateTransferThrottle(),
                        sender.getStateTransferThreadsCount()
                    );

                final GridDrReceiverCacheConfiguration receiver = cacheCfg.getDrReceiverConfiguration();

                VisorDrReceiverConfig drReceiverCfg = null;

                if (receiver != null)
                    drReceiverCfg = new VisorDrReceiverConfig(
                        compactClass(receiver.getConflictResolver()),
                        receiver.getConflictResolverMode()
                    );

                caches.add(new VisorCacheConfig(
                    cacheCfg.getName(),
                    cacheCfg.getCacheMode(),
                    cacheCfg.getDistributionMode(),
                    cacheCfg.getAtomicityMode(),
                    cacheCfg.getAtomicSequenceReserveSize(),
                    cacheCfg.getAtomicWriteOrderMode(),
                    cacheCfg.getDefaultTimeToLive(),
                    cacheCfg.isEagerTtl(),
                    cacheCfg.getRefreshAheadRatio(),
                    cacheCfg.getWriteSynchronizationMode(),
                    cacheCfg.getAtomicSequenceReserveSize(),
                    cacheCfg.isSwapEnabled(),
                    cacheCfg.isQueryIndexEnabled(),
                    cacheCfg.isBatchUpdateOnCommit(),
                    cacheCfg.isInvalidate(),
                    cacheCfg.getStartSize(),
                    compactClass(cacheCfg.getCloner()),
                    cacheCfg.getTransactionManagerLookupClassName(),
                    cacheCfg.isTxSerializableEnabled(),
                    cacheCfg.getOffHeapMaxMemory(),
                    cacheCfg.getMaximumQueryIteratorCount(),
                    cacheCfg.getMaxConcurrentAsyncOperations(),
                    cacheCfg.getPessimisticTxLogSize(),
                    cacheCfg.getPessimisticTxLogLinger(),
                    cacheCfg.getMemoryMode(),
                    cacheCfg.getIndexingSpiName(),
                    affinity,
                    preload,
                    evict,
                    near,
                    dflt,
                    dgc,
                    store,
                    writeBehind,
                    drSenderCfg,
                    drReceiverCfg));
            }

            final List<VisorGgfsConfig> ggfss = new ArrayList<>();

            if (c.getGgfsConfiguration() != null)
                for (GridGgfsConfiguration ggfs : c.getGgfsConfiguration()) {
                    ggfss.add(new VisorGgfsConfig(
                        ggfs.getName(),
                        ggfs.getMetaCacheName(),
                        ggfs.getDataCacheName(),
                        ggfs.getBlockSize(),
                        ggfs.getPrefetchBlocks(),
                        ggfs.getStreamBufferSize(),
                        ggfs.getPerNodeBatchSize(),
                        ggfs.getPerNodeParallelBatchCount(),
                        ggfs.getSecondaryHadoopFileSystemUri(),
                        ggfs.getSecondaryHadoopFileSystemConfigPath(),
                        ggfs.getDefaultMode(),
                        ggfs.getPathModes(),
                        compactClass(ggfs.getDualModePutExecutorService()),
                        ggfs.getDualModePutExecutorServiceShutdown(),
                        ggfs.getDualModeMaxPendingPutsSize(),
                        ggfs.getMaximumTaskRangeLength(),
                        ggfs.getFragmentizerConcurrentFiles(),
                        ggfs.getFragmentizerLocalWritesRatio(),
                        ggfs.isFragmentizerEnabled(),
                        ggfs.getFragmentizerThrottlingBlockLength(),
                        ggfs.getFragmentizerThrottlingDelay(),
                        ggfs.getIpcEndpointConfiguration(),
                        ggfs.isIpcEndpointEnabled(),
                        ggfs.getMaxSpaceSize(),
                        ggfs.getManagementPort(),
                        ggfs.getSequentialReadsBeforePrefetch(),
                        ggfs.getTrashPurgeTimeout()
                    ));
                }

            final List<VisorStreamerConfig> streamers = new ArrayList<>();

            if (c.getStreamerConfiguration() != null)
                for (GridStreamerConfiguration streamer : c.getStreamerConfiguration())
                    streamers.add(new VisorStreamerConfig(
                        streamer.getName(),
                        compactClass(streamer.getRouter()),
                        streamer.isAtLeastOnce(),
                        streamer.getMaximumFailoverAttempts(),
                        streamer.getMaximumConcurrentSessions(),
                        streamer.isExecutorServiceShutdown()
                    ));

            VisorDrSenderHubConfig senderHub = null;

            if (c.getDrSenderHubConfiguration() != null) {
                GridDrSenderHubConfiguration hCfg = c.getDrSenderHubConfiguration();

                List<VisorDrSenderHubConnectionConfig> hubConnections = new ArrayList<>();

                for (GridDrSenderHubConnectionConfiguration cCfg : hCfg.getConnectionConfiguration()) {
                    hubConnections.add(new VisorDrSenderHubConnectionConfig(
                        cCfg.getDataCenterId(),
                        cCfg.getReceiverHubAddresses(),
                        cCfg.getLocalOutboundHost(),
                        cCfg.getReceiverHubLoadBalancingMode(),
                        cCfg.getIgnoredDataCenterIds()
                    ));
                }

                senderHub = new VisorDrSenderHubConfig(
                    hubConnections,
                    hCfg.getMaxFailedConnectAttempts(),
                    hCfg.getMaxErrors(),
                    hCfg.getHealthCheckFrequency(),
                    hCfg.getSystemRequestTimeout(),
                    hCfg.getReadTimeout(),
                    hCfg.getMaxQueueSize(),
                    hCfg.getReconnectOnFailureTimeout(),
                    hCfg.getCacheNames()
                );
            }

            VisorDrReceiverHubConfig receiverHub = null;

            if (c.getDrReceiverHubConfiguration() != null) {
                GridDrReceiverHubConfiguration hCfg = c.getDrReceiverHubConfiguration();

                receiverHub = new VisorDrReceiverHubConfig(
                    hCfg.getLocalInboundHost(),
                    hCfg.getLocalInboundPort(),
                    hCfg.getSelectorCount(),
                    hCfg.getWorkerThreads(),
                    hCfg.getMessageQueueLimit(),
                    hCfg.isTcpNodelay(),
                    hCfg.isDirectBuffer(),
                    hCfg.getIdleTimeout(),
                    hCfg.getWriteTimeout(),
                    hCfg.getFlushFrequency(),
                    hCfg.getPerNodeBufferSize(),
                    hCfg.getPerNodeParallelLoadOperations()
                );
            }

            return new VisorGridConfig(
                lic,
                basic,
                metrics,
                spis,
                p2p,
                email,
                lifecycle,
                execSvc,
                seg,
                compactArray(c.getIncludeProperties()),
                c.getIncludeEventTypes(),
                rest,
                c.getUserAttributes(),
                caches,
                ggfss,
                streamers,
                senderHub,
                receiverHub,
                new HashMap<>(getenv()),
                getProperties()
            );
        }
    }

    @Override protected VisorConfigurationJob job(VisorOneNodeArg arg) {
        return new VisorConfigurationJob(arg);
    }
}
