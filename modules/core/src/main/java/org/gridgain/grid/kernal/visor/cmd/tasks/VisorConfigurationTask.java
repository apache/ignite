/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.tasks;

import static java.lang.System.*;
import static org.gridgain.grid.GridSystemProperties.*;

import java.io.*;
import java.util.*;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.kernal.visor.cmd.dto.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Grid configuration data collect task.
 */
@GridInternal
public class VisorConfigurationTask extends VisorOneNodeTask<VisorOneNodeArg, VisorConfigurationTask.VisorConfiguration> {
    /**
     * Grid configuration data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorConfiguration implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** License. */
        private final GridProductLicense license;

        /** Basic. */
        private final VisorBasicConfig basic;

        /** Metrics. */
        private final VisorMetricsConfig metrics;

        /** SPIs. */
        private final VisorSpisConfig spis;

        /** P2P. */
        private final VisorPeerToPeerConfig p2p;

        /** Email. */
        private final VisorEmailConfig email;

        /** Lifecycle. */
        private final VisorLifecycleConfig lifecycle;

        /** Executors service configuration. */
        private final VisorExecServiceConfig execSvc;

        /** Segmentation. */
        private final VisorSegmentationConfig seg;

        /** Include properties. */
        private final String inclProps;

        /** Include events types. */
        private final int[] inclEvtTypes;

        private final VisorRestConfig rest;

        /** User attributes. */
        private final Map<String, ?> userAttrs;

        /** Caches. */
        private final Iterable<VisorCacheConfig> caches;

        /** Environment. */
        private final Map<String, String> env;

        /** System properties. */
        private final Properties sysProps;

        public VisorConfiguration(GridProductLicense license, VisorBasicConfig basic, VisorMetricsConfig metrics,
            VisorSpisConfig spis, VisorPeerToPeerConfig p2p, VisorEmailConfig email, VisorLifecycleConfig lifecycle,
            VisorExecServiceConfig execSvc, VisorSegmentationConfig seg, String inclProps, int[] inclEvtTypes,
            VisorRestConfig rest, Map<String, ?> userAttrs, Iterable<VisorCacheConfig> caches, Map<String, String> env,
            Properties sysProps) {
            this.license = license;
            this.basic = basic;
            this.metrics = metrics;
            this.spis = spis;
            this.p2p = p2p;
            this.email = email;
            this.lifecycle = lifecycle;
            this.execSvc = execSvc;
            this.seg = seg;
            this.inclProps = inclProps;
            this.inclEvtTypes = inclEvtTypes;
            this.rest = rest;
            this.userAttrs = userAttrs;
            this.caches = caches;
            this.env = env;
            this.sysProps = sysProps;
        }

        /**
         * @return License.
         */
        public GridProductLicense license() {
            return license;
        }

        /**
         * @return Basic.
         */
        public VisorBasicConfig basic() {
            return basic;
        }

        /**
         * @return Metric.
         */
        public VisorMetricsConfig metrics() {
            return metrics;
        }

        /**
         * @return Spis.
         */
        public VisorSpisConfig spis() {
            return spis;
        }

        /**
         * @return P2P.
         */
        public VisorPeerToPeerConfig p2p() {
            return p2p;
        }

        /**
         * @return Email.
         */
        public VisorEmailConfig email() {
            return email;
        }

        /**
         * @return Lifecycle.
         */
        public VisorLifecycleConfig lifecycle() {
            return lifecycle;
        }

        /**
         * @return Executors.
         */
        public VisorExecServiceConfig executeSvc() {
            return execSvc;
        }

        /**
         * @return Segmentation.
         */
        public VisorSegmentationConfig seg() {
            return seg;
        }

        /**
         * @return Include properties.
         */
        public String inclProperties() {
            return inclProps;
        }

        /**
         * @return Include events types.
         */
        public int[] inclEventTypes() {
            return inclEvtTypes;
        }

        /**
         * @return Rest.
         */
        public VisorRestConfig rest() {
            return rest;
        }

        /**
         * @return User attributes.
         */
        public Map<String, ?> userAttributes() {
            return userAttrs;
        }

        /**
         * @return Caches.
         */
        public Iterable<VisorCacheConfig> caches() {
            return caches;
        }

        /**
         * @return Environment.
         */
        public Map<String, String> env() {
            return env;
        }

        /**
         * @return System properties.
         */
        public Properties systemProperties() {
            return sysProps;
        }
    }

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
    private static String compactObject(Object obj) {
        return (obj != null) ? U.compact(obj.getClass().getName()) : null;
    }

    /**
     * Joins array elements to string.
     *
     * @param arr Array.
     * @return String.
     */
    private static String compactArray(Object[] arr) {
        if (arr == null && arr.length == 0)
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
    public static class VisorConfigurationJob extends VisorOneNodeJob<VisorOneNodeArg, VisorConfiguration> {
        /** */
        private static final long serialVersionUID = 0L;

        public VisorConfigurationJob(VisorOneNodeArg arg) {
            super(arg);
        }

        @Override protected VisorConfiguration run(VisorOneNodeArg arg) {
            final GridConfiguration c = g.configuration();
            final GridProductLicense lic = g.product().license();

            final VisorBasicConfig basic = new VisorBasicConfig(c.getGridName(),
                getProperty(GG_HOME, c.getGridGainHome()),
                getProperty(GG_LOCAL_HOST, c.getLocalHost()),
                g.localNode().id(),
                compactObject(c.getMarshaller()),
                compactObject(c.getDeploymentMode()),
                boolValue(GG_DAEMON, c.isDaemon()),
                g.isJmxRemoteEnabled(),
                g.isRestartEnabled(),
                c.getNetworkTimeout(),
                c.getLicenseUrl(),
                compactObject(c.getGridLogger()),
                c.getDiscoveryStartupDelay(),
                compactObject(c.getMBeanServer()),
                boolValue(GG_NO_ASCII, false),
                boolValue(GG_NO_DISCO_ORDER, false),
                boolValue(GG_NO_SHUTDOWN_HOOK, false),
                getProperty(GG_PROG_NAME),
                boolValue(GG_QUIET, true),
                getProperty(GG_SUCCESS_FILE),
                boolValue(GG_UPDATE_NOTIFIER, true));

            final VisorMetricsConfig metrics = new VisorMetricsConfig(c.getMetricsExpireTime(),
                c.getMetricsHistorySize(),
                c.getMetricsLogFrequency());

            final VisorSpisConfig spis = new VisorSpisConfig(compactObject(c.getDiscoverySpi()),
                compactObject(c.getCommunicationSpi()),
                compactObject(c.getEventStorageSpi()),
                compactObject(c.getCollisionSpi()),
                compactObject(c.getAuthenticationSpi()),
                compactObject(c.getSecureSessionSpi()),
                compactObject(c.getDeploymentSpi()),
                compactArray(c.getCheckpointSpi()),
                compactArray(c.getFailoverSpi()),
                compactArray(c.getLoadBalancingSpi()),
                compactObject(c.getSwapSpaceSpi())
            );

            final VisorPeerToPeerConfig p2p = new VisorPeerToPeerConfig(c.isPeerClassLoadingEnabled(),
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
                boolValue(GG_LIFECYCLE_EMAIL_NOTIFY, c.isLifeCycleEmailNotification())
            );

            final VisorExecServiceConfig execSvc = new VisorExecServiceConfig(
                compactObject(c.getExecutorService()),
                c.getExecutorServiceShutdown(),
                compactObject(c.getSystemExecutorService()),
                c.getSystemExecutorServiceShutdown(),
                compactObject(c.getPeerClassLoadingExecutorService()),
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
                compactObject(c.getRestTcpSslContextFactory())
            );

            final List<VisorCacheConfig> caches = Collections.emptyList();

            for (GridCacheConfiguration cacheCfg : c.getCacheConfiguration()) {
                VisorAffinityConfig affinity = new VisorAffinityConfig(
                    compactObject(cacheCfg.getAffinity()),
                    compactObject(cacheCfg.getAffinityMapper())
                );

                VisorPreloadConfig preload = new VisorPreloadConfig(
                    cacheCfg.getPreloadMode(),
                    cacheCfg.getPreloadBatchSize(),
                    cacheCfg.getPreloadThreadPoolSize()
                );

                VisorEvictionConfig evict = new VisorEvictionConfig(
                    compactObject(cacheCfg.getEvictionPolicy()),
                    cacheCfg.getEvictSynchronizedKeyBufferSize(),
                    cacheCfg.isEvictSynchronized(),
                    cacheCfg.isEvictNearSynchronized(),
                    cacheCfg.getEvictMaxOverflowRatio());

                VisorNearCacheConfig near = new VisorNearCacheConfig(
                    GridCacheUtils.isNearEnabled(cacheCfg),
                    cacheCfg.getNearStartSize(),
                    compactObject(cacheCfg.getNearEvictionPolicy())
                );

                VisorDefaultCacheConfig dflt = new VisorDefaultCacheConfig(
                    cacheCfg.getDefaultTxIsolation(),
                    cacheCfg.getDefaultTxConcurrency(),
                    cacheCfg.getDefaultTxTimeout(),
                    cacheCfg.getDefaultLockTimeout()
                );

                VisorDgcConfig dgc = new VisorDgcConfig(
                    cacheCfg.getDgcFrequency(),
                    cacheCfg.isDgcRemoveLocks(),
                    cacheCfg.getDgcSuspectLockTimeout()
                );

                VisorStoreConfig store = new VisorStoreConfig(
                    compactObject(cacheCfg.getStore()),
                    cacheCfg.isStoreValueBytes()
                );

                caches.add(new VisorCacheConfig(cacheCfg.getName(),
                    cacheCfg.getCacheMode(),
                    cacheCfg.getDefaultTimeToLive(),
                    cacheCfg.getRefreshAheadRatio(),
                    cacheCfg.getAtomicSequenceReserveSize(),
                    cacheCfg.isSwapEnabled(),
                    cacheCfg.isBatchUpdateOnCommit(),
                    cacheCfg.isInvalidate(),
                    cacheCfg.getStartSize(),
                    compactObject(cacheCfg.getCloner()),
                    cacheCfg.getTransactionManagerLookupClassName(),
                    affinity,
                    preload,
                    evict,
                    near,
                    dflt,
                    dgc,
                    store
                ));
            }

            return new VisorConfiguration(lic,
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
                getenv(),
                getProperties());
        }
    }

    @Override protected VisorConfigurationJob job(VisorOneNodeArg arg) {
        return new VisorConfigurationJob(arg);
    }
}
