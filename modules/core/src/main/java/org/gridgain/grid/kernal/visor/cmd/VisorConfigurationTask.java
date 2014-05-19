/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd;

import static java.lang.System.*;
import static org.gridgain.grid.GridSystemProperties.*;

import java.io.*;
import java.util.*;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.segmentation.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * TODO: Add class description.
 */
@GridInternal
public class VisorConfigurationTask extends VisorOneNodeTask<VisorOneNodeArg, VisorConfigurationTask.VisorConfiguration> {
    /**
     * Basic configuration data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorBasicConfig implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final String gridName;
        private final String ggHome;
        private final String locHost;
        private final  UUID nodeId;
        private final String marsh;
        private final String deployMode;
        private final boolean daemon;
        private final boolean jmxRemote;
        private final boolean restart;
        private final long netTimeout;
        private final String licenseUrl;
        private final String log;
        private final long discoStartupDelay;
        private final String mBeanSrv;
        private final boolean noAscii;
        private final boolean noDiscoOrder;
        private final boolean noShutdownHook;
        private final String progName;
        private final boolean quiet;
        private final String successFile;
        private final boolean updateNtf;

        public VisorBasicConfig(String gridName, String ggHome, String locHost, UUID nodeId, String marsh,
            String deployMode, boolean daemon, boolean jmxRemote, boolean restart, long netTimeout, String licenseUrl,
            String log, long discoStartupDelay, String mBeanSrv, boolean noAscii, boolean noDiscoOrder,
            boolean noShutdownHook, String progName, boolean quiet, String successFile, boolean updateNtf) {
            this.gridName = gridName;
            this.ggHome = ggHome;
            this.locHost = locHost;
            this.nodeId = nodeId;
            this.marsh = marsh;
            this.deployMode = deployMode;
            this.daemon = daemon;
            this.jmxRemote = jmxRemote;
            this.restart = restart;
            this.netTimeout = netTimeout;
            this.licenseUrl = licenseUrl;
            this.log = log;
            this.discoStartupDelay = discoStartupDelay;
            this.mBeanSrv = mBeanSrv;
            this.noAscii = noAscii;
            this.noDiscoOrder = noDiscoOrder;
            this.noShutdownHook = noShutdownHook;
            this.progName = progName;
            this.quiet = quiet;
            this.successFile = successFile;
            this.updateNtf = updateNtf;
        }

        /**
         * @return Grid name.
         */
        public String gridName() {
            return gridName;
        }

        /**
         * @return GridGain home.
         */
        public String ggHome() {
            return ggHome;
        }

        /**
         * @return Locale host.
         */
        public String localeHost() {
            return locHost;
        }

        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Marshaller.
         */
        public String marsh() {
            return marsh;
        }

        /**
         * @return Deploy mode.
         */
        public String deployMode() {
            return deployMode;
        }

        /**
         * @return Is daemon node.
         */
        public boolean daemon() {
            return daemon;
        }

        /**
         * @return Jmx remote.
         */
        public boolean jmxRemote() {
            return jmxRemote;
        }

        /**
         * @return Is restart supported.
         */
        public boolean restart() {
            return restart;
        }

        /**
         * @return Network timeout.
         */
        public long networkTimeout() {
            return netTimeout;
        }

        /**
         * @return License url.
         */
        public String licenseUrl() {
            return licenseUrl;
        }

        /**
         * @return Logger.
         */
        public String logger() {
            return log;
        }

        /**
         * @return Disco startup delay.
         */
        public long discoStartupDelay() {
            return discoStartupDelay;
        }

        /**
         * @return M bean server.
         */
        public String mBeanServer() {
            return mBeanSrv;
        }

        /**
         * @return No ascii.
         */
        public boolean noAscii() {
            return noAscii;
        }

        /**
         * @return No disco order.
         */
        public boolean noDiscoOrder() {
            return noDiscoOrder;
        }

        /**
         * @return No shutdown hook.
         */
        public boolean noShutdownHook() {
            return noShutdownHook;
        }

        /**
         * @return Prog name.
         */
        public String progName() {
            return progName;
        }

        /**
         * @return Quiet.
         */
        public boolean quiet() {
            return quiet;
        }

        /**
         * @return Success file.
         */
        public String successFile() {
            return successFile;
        }

        /**
         * @return Update notifier.
         */
        public boolean updateNotifier() {
            return updateNtf;
        }
    }

    /**
     * Metrics configuration data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorMetricsConfig implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final long expTime;
        private final int historySize;
        private final long logFreq;

        public VisorMetricsConfig(long expTime, int historySize, long logFreq) {
            this.expTime = expTime;
            this.historySize = historySize;
            this.logFreq = logFreq;
        }

        /**
         * @return Expected time.
         */
        public long expectedTime() {
            return expTime;
        }

        /**
         * @return History size.
         */
        public int historySize() {
            return historySize;
        }

        /**
         * @return Logger frequency.
         */
        public long loggerFrequency() {
            return logFreq;
        }
    }

    /**
     * SPIs configuration data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorSpisConfig implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final String discoSpi;
        private final String commSpi;
        private final String evtSpi;
        private final String colSpi;
        private final String authSpi;
        private final String sesSpi;
        private final String deploySpi;
        private final String cpSpis;
        private final String failSpis;
        private final String loadBalancingSpis;
        private final String swapSpaceSpis;

        public VisorSpisConfig(String discoSpi, String commSpi, String evtSpi, String colSpi, String authSpi,
            String sesSpi, String deploySpi, String cpSpis, String failSpis, String loadBalancingSpis,
            String swapSpaceSpis) {
            this.discoSpi = discoSpi;
            this.commSpi = commSpi;
            this.evtSpi = evtSpi;
            this.colSpi = colSpi;
            this.authSpi = authSpi;
            this.sesSpi = sesSpi;
            this.deploySpi = deploySpi;
            this.cpSpis = cpSpis;
            this.failSpis = failSpis;
            this.loadBalancingSpis = loadBalancingSpis;
            this.swapSpaceSpis = swapSpaceSpis;
        }

        /**
         * @return Disco spi.
         */
        public String discoSpi() {
            return discoSpi;
        }

        /**
         * @return Communication spi.
         */
        public String communicationSpi() {
            return commSpi;
        }

        /**
         * @return Event spi.
         */
        public String eventSpi() {
            return evtSpi;
        }

        /**
         * @return Column spi.
         */
        public String columnSpi() {
            return colSpi;
        }

        /**
         * @return Auth spi.
         */
        public String authSpi() {
            return authSpi;
        }

        /**
         * @return Session spi.
         */
        public String sessionSpi() {
            return sesSpi;
        }

        /**
         * @return Deploy spi.
         */
        public String deploySpi() {
            return deploySpi;
        }

        /**
         * @return Copy spis.
         */
        public String cpSpis() {
            return cpSpis;
        }

        /**
         * @return Fail spis.
         */
        public String failSpis() {
            return failSpis;
        }

        /**
         * @return Load balancing spis.
         */
        public String loadBalancingSpis() {
            return loadBalancingSpis;
        }

        /**
         * @return Swap space spis.
         */
        public String swapSpaceSpis() {
            return swapSpaceSpis;
        }
    }

    /**
     * P2P configuration data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorPeerToPeerConfig implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final boolean p2pEnabled;
        private final int p2pMissedResCacheSize;
        private final String p2pLocClsPathExcl;

        public VisorPeerToPeerConfig(boolean p2pEnabled, int p2pMissedResCacheSize, String p2pLocClsPathExcl) {
            this.p2pEnabled = p2pEnabled;
            this.p2pMissedResCacheSize = p2pMissedResCacheSize;
            this.p2pLocClsPathExcl = p2pLocClsPathExcl;
        }

        /**
         * @return P2p enabled.
         */
        public boolean p2PEnabled() {
            return p2pEnabled;
        }

        /**
         * @return P2p missed response cache size.
         */
        public int p2PMissedResponseCacheSize() {
            return p2pMissedResCacheSize;
        }

        /**
         * @return P2p locale class path excl.
         */
        public String p2PLocaleClassPathExcl() {
            return p2pLocClsPathExcl;
        }
    }

    /**
     * Email configuration data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorEmailConfig implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final String smtpHost;
        private final int smtpPort;
        private final String smtpUsername;
        private final String adminEmails;
        private final String smtpFromEmail;
        private final boolean smtpSsl;
        private final boolean smtpStartTls;

        public VisorEmailConfig(String smtpHost, int smtpPort, String smtpUsername, String adminEmails,
            String smtpFromEmail, boolean smtpSsl, boolean smtpStartTls) {
            this.smtpHost = smtpHost;
            this.smtpPort = smtpPort;
            this.smtpUsername = smtpUsername;
            this.adminEmails = adminEmails;
            this.smtpFromEmail = smtpFromEmail;
            this.smtpSsl = smtpSsl;
            this.smtpStartTls = smtpStartTls;
        }

        /**
         * @return Smtp host.
         */
        public String smtpHost() {
            return smtpHost;
        }

        /**
         * @return Smtp port.
         */
        public int smtpPort() {
            return smtpPort;
        }

        /**
         * @return Smtp username.
         */
        public String smtpUsername() {
            return smtpUsername;
        }

        /**
         * @return Administration emails.
         */
        public String administrationEmails() {
            return adminEmails;
        }

        /**
         * @return Smtp from email.
         */
        public String smtpFromEmail() {
            return smtpFromEmail;
        }

        /**
         * @return Smtp ssl.
         */
        public boolean smtpSsl() {
            return smtpSsl;
        }

        /**
         * @return Smtp start tls.
         */
        public boolean smtpStartTls() {
            return smtpStartTls;
        }
    }

    /**
     * Lifecycle configuration data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorLifecycleConfig implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final String beans;
        private final boolean ntf;

        public VisorLifecycleConfig(String beans, boolean ntf) {
            this.beans = beans;
            this.ntf = ntf;
        }

        /**
         * @return Beans.
         */
        public String beans() {
            return beans;
        }

        /**
         * @return Notifier.
         */
        public boolean notifier() {
            return ntf;
        }
    }

    /**
     * Executors configuration data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorExecServiceConfig implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final String execSvc;
        private final boolean execSvcShutdown;
        private final String sysExecSvc;
        private final boolean sysExecSvcShutdown;
        private final String p2pExecSvc;
        private final boolean p2pExecSvcShutdown;

        public VisorExecServiceConfig(String execSvc, boolean execSvcShutdown, String sysExecSvc,
            boolean sysExecSvcShutdown, String p2pExecSvc, boolean p2pExecSvcShutdown) {
            this.execSvc = execSvc;
            this.execSvcShutdown = execSvcShutdown;
            this.sysExecSvc = sysExecSvc;
            this.sysExecSvcShutdown = sysExecSvcShutdown;
            this.p2pExecSvc = p2pExecSvc;
            this.p2pExecSvcShutdown = p2pExecSvcShutdown;
        }

        /**
         * @return Execute svc.
         */
        public String executeSvc() {
            return execSvc;
        }

        /**
         * @return Execute svc shutdown.
         */
        public boolean executeSvcShutdown() {
            return execSvcShutdown;
        }

        /**
         * @return System execute svc.
         */
        public String systemExecuteSvc() {
            return sysExecSvc;
        }

        /**
         * @return System execute svc shutdown.
         */
        public boolean systemExecuteSvcShutdown() {
            return sysExecSvcShutdown;
        }

        /**
         * @return P2p execute svc.
         */
        public String p2pExecuteSvc() {
            return p2pExecSvc;
        }

        /**
         * @return P2p execute svc shutdown.
         */
        public boolean p2pExecuteSvcShutdown() {
            return p2pExecSvcShutdown;
        }
    }

    /**
     * Segmentation configuration data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorSegmentationConfig implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final GridSegmentationPolicy plc;
        private final String resolvers;
        private final long checkFreq;
        private final boolean waitOnStart;
        private final boolean passRequired;

        public VisorSegmentationConfig(GridSegmentationPolicy plc, String resolvers, long checkFreq, boolean waitOnStart,
            boolean passRequired) {
            this.plc = plc;
            this.resolvers = resolvers;
            this.checkFreq = checkFreq;
            this.waitOnStart = waitOnStart;
            this.passRequired = passRequired;
        }

        /**
         * @return Policy.
         */
        public GridSegmentationPolicy policy() {
            return plc;
        }

        /**
         * @return Resolvers.
         */
        public String resolvers() {
            return resolvers;
        }

        /**
         * @return Check frequency.
         */
        public long checkFrequency() {
            return checkFreq;
        }

        /**
         * @return Wait on start.
         */
        public boolean waitOnStart() {
            return waitOnStart;
        }

        /**
         * @return Pass required.
         */
        public boolean passRequired() {
            return passRequired;
        }
    }

    /**
     * Affinity configuration data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorAffinityConfig implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final String affinity;
        private final String affinityMapper;

        public VisorAffinityConfig(String affinity, String affinityMapper) {
            this.affinity = affinity;
            this.affinityMapper = affinityMapper;
        }

        /**
         * @return Affinity.
         */
        public String affinity() {
            return affinity;
        }

        /**
         * @return Affinity mapper.
         */
        public String affinityMapper() {
            return affinityMapper;
        }
    }

    /**
     * Preload configuration data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorPreloadConfig implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final GridCachePreloadMode mode;
        private final int poolSize;
        private final int batchSize;

        public VisorPreloadConfig(GridCachePreloadMode mode, int poolSize, int batchSize) {
            this.mode = mode;
            this.poolSize = poolSize;
            this.batchSize = batchSize;
        }

        /**
         * @return Mode.
         */
        public GridCachePreloadMode mode() {
            return mode;
        }

        /**
         * @return Pool size.
         */
        public int poolSize() {
            return poolSize;
        }

        /**
         * @return Batch size.
         */
        public int batchSize() {
            return batchSize;
        }
    }

    /**
     * Eviction configuration data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorEvictionConfig implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final String plc;
        private final int keyBufSize;
        private final boolean evictSynchronized;
        private final boolean nearSynchronized;
        private final float maxOverflowRatio;

        public VisorEvictionConfig(String plc, int keyBufSize, boolean evictSynchronized, boolean nearSynchronized,
            float maxOverflowRatio) {
            this.plc = plc;
            this.keyBufSize = keyBufSize;
            this.evictSynchronized = evictSynchronized;
            this.nearSynchronized = nearSynchronized;
            this.maxOverflowRatio = maxOverflowRatio;
        }

        /**
         * @return Policy.
         */
        public String policy() {
            return plc;
        }

        /**
         * @return Key buffer size.
         */
        public int keyBufferSize() {
            return keyBufSize;
        }

        /**
         * @return Evict synchronized.
         */
        public boolean evictSynchronized() {
            return evictSynchronized;
        }

        /**
         * @return Near synchronized.
         */
        public boolean nearSynchronized() {
            return nearSynchronized;
        }

        /**
         * @return Max overflow ratio.
         */
        public float maxOverflowRatio() {
            return maxOverflowRatio;
        }
    }

    /**
     * Near cache configuration data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorNearCacheConfig implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final boolean nearEnabled;
        private final int nearStartSize;
        private final String nearEvictPlc;

        public VisorNearCacheConfig(boolean nearEnabled, int nearStartSize, String nearEvictPlc) {
            this.nearEnabled = nearEnabled;
            this.nearStartSize = nearStartSize;
            this.nearEvictPlc = nearEvictPlc;
        }

        /**
         * @return Near enabled.
         */
        public boolean nearEnabled() {
            return nearEnabled;
        }

        /**
         * @return Near start size.
         */
        public int nearStartSize() {
            return nearStartSize;
        }

        /**
         * @return Near evict policy.
         */
        public String nearEvictPolicy() {
            return nearEvictPlc;
        }
    }

    /**
     * Default cache configuration data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorDefaultCacheConfig implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final GridCacheTxIsolation dfltIsolation;
        private final GridCacheTxConcurrency dfltConcurrency;
        private final long dfltTxTimeout;
        private final long dfltLockTimeout;

        public VisorDefaultCacheConfig(GridCacheTxIsolation dfltIsolation,
            GridCacheTxConcurrency dfltConcurrency, long dfltTxTimeout, long dfltLockTimeout) {
            this.dfltIsolation = dfltIsolation;
            this.dfltConcurrency = dfltConcurrency;
            this.dfltTxTimeout = dfltTxTimeout;
            this.dfltLockTimeout = dfltLockTimeout;
        }

        /**
         * @return Default isolation.
         */
        public GridCacheTxIsolation defaultIsolation() {
            return dfltIsolation;
        }

        /**
         * @return Default concurrency.
         */
        public GridCacheTxConcurrency defaultConcurrency() {
            return dfltConcurrency;
        }

        /**
         * @return Default tx timeout.
         */
        public long defaultTxTimeout() {
            return dfltTxTimeout;
        }

        /**
         * @return Default lock timeout.
         */
        public long defaultLockTimeout() {
            return dfltLockTimeout;
        }
    }

    /**
     * DGC configuration data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorDgcConfig implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final long freq;
        private final boolean rmvLocks;
        private final long suspectLockTimeout;

        public VisorDgcConfig(long freq, boolean rmvLocks, long suspectLockTimeout) {
            this.freq = freq;
            this.rmvLocks = rmvLocks;
            this.suspectLockTimeout = suspectLockTimeout;
        }

        /**
         * @return Frequency.
         */
        public long frequency() {
            return freq;
        }

        /**
         * @return Removed locks.
         */
        public boolean removedLocks() {
            return rmvLocks;
        }

        /**
         * @return Suspect lock timeout.
         */
        public long suspectLockTimeout() {
            return suspectLockTimeout;
        }
    }

    /**
     * Store configuration data.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorStoreConfig implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final String store;
        private final boolean valueBytes;

        public VisorStoreConfig(String store, boolean valueBytes) {
            this.store = store;
            this.valueBytes = valueBytes;
        }

        public boolean enabled() {
            return store != null;
        }

        /**
         * @return Store.
         */
        public String store() {
            return store;
        }

        /**
         * @return Value bytes.
         */
        public boolean valueBytes() {
            return valueBytes;
        }
    }

    @SuppressWarnings("PublicInnerClass")
    public static class VisorCacheConfig implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final String name;
        private final GridCacheMode mode;
        private final long ttl;
        private final double refreshAheadRatio;
        private final int seqReserveSize;
        private final boolean swapEnabled;
        private final boolean txBatchUpdate;
        private final boolean invalidate;
        private final int startSize;
        private final String cloner;
        private final String txMgrLookup;

        private final VisorAffinityConfig affinity;
        private final VisorPreloadConfig preload;
        private final VisorEvictionConfig evict;
        private final VisorNearCacheConfig near;
        private final VisorDefaultCacheConfig dflt;
        private final VisorDgcConfig dgc;
        private final VisorStoreConfig store;

        public VisorCacheConfig(String name, GridCacheMode mode, long ttl, double refreshAheadRatio, int seqReserveSize,
            boolean swapEnabled, boolean txBatchUpdate, boolean invalidate, int startSize, String cloner,
            String txMgrLookup, VisorAffinityConfig affinity,
            VisorPreloadConfig preload, VisorEvictionConfig evict,
            VisorNearCacheConfig near, VisorDefaultCacheConfig dflt,
            VisorDgcConfig dgc, VisorStoreConfig store) {
            this.name = name;
            this.mode = mode;
            this.ttl = ttl;
            this.refreshAheadRatio = refreshAheadRatio;
            this.seqReserveSize = seqReserveSize;
            this.swapEnabled = swapEnabled;
            this.txBatchUpdate = txBatchUpdate;
            this.invalidate = invalidate;
            this.startSize = startSize;
            this.cloner = cloner;
            this.txMgrLookup = txMgrLookup;
            this.affinity = affinity;
            this.preload = preload;
            this.evict = evict;
            this.near = near;
            this.dflt = dflt;
            this.dgc = dgc;
            this.store = store;
        }

        /**
         * @return Name.
         */
        public String name() {
            return name;
        }

        /**
         * @return Mode.
         */
        public GridCacheMode mode() {
            return mode;
        }

        /**
         * @return Ttl.
         */
        public long ttl() {
            return ttl;
        }

        /**
         * @return Refresh ahead ratio.
         */
        public double refreshAheadRatio() {
            return refreshAheadRatio;
        }

        /**
         * @return Sequence reserve size.
         */
        public int sequenceReserveSize() {
            return seqReserveSize;
        }

        /**
         * @return Swap enabled.
         */
        public boolean swapEnabled() {
            return swapEnabled;
        }

        /**
         * @return Tx batch update.
         */
        public boolean txBatchUpdate() {
            return txBatchUpdate;
        }

        /**
         * @return Invalidate.
         */
        public boolean invalidate() {
            return invalidate;
        }

        /**
         * @return Start size.
         */
        public int startSize() {
            return startSize;
        }

        /**
         * @return Cloner.
         */
        public String cloner() {
            return cloner;
        }

        /**
         * @return Tx manager lookup.
         */
        public String txManagerLookup() {
            return txMgrLookup;
        }

        /**
         * @return Affinity.
         */
        public VisorAffinityConfig affinity() {
            return affinity;
        }

        /**
         * @return Preload.
         */
        public VisorPreloadConfig preload() {
            return preload;
        }

        /**
         * @return Evict.
         */
        public VisorEvictionConfig evict() {
            return evict;
        }

        /**
         * @return Near.
         */
        public VisorNearCacheConfig near() {
            return near;
        }

        /**
         * @return dflt.
         */
        public VisorDefaultCacheConfig defaultConfig() {
            return dflt;
        }

        /**
         * @return Dgc.
         */
        public VisorDgcConfig dgc() {
            return dgc;
        }

        /**
         * @return Store.
         */
        public VisorStoreConfig store() {
            return store;
        }
    }

    @SuppressWarnings("PublicInnerClass")
    public static class VisorConfiguration implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final GridProductLicense license;

        private final VisorBasicConfig basic;

        private final VisorMetricsConfig metrics;

        private final VisorSpisConfig spis;

        private final VisorPeerToPeerConfig p2p;

        private final VisorEmailConfig email;

        private final VisorLifecycleConfig lifecycle;

        private final VisorExecServiceConfig execSvc;

        private final VisorSegmentationConfig seg;

        private final String inclProps;

        private final int[] inclEvtTypes;

        private final boolean restEnabled;

        private final String jettyPath;

        private final String jettyHost;

        private final Integer jettyPort;

        private final Map<String, ?> userAttrs;

        private final Iterable<VisorCacheConfig> caches;

        private final Map<String, String> env;

        private final Properties sysProps;

        public VisorConfiguration(GridProductLicense license,
            VisorBasicConfig basic, VisorMetricsConfig metrics,
            VisorSpisConfig spis, VisorPeerToPeerConfig p2p,
            VisorEmailConfig email, VisorLifecycleConfig lifecycle,
            VisorExecServiceConfig execSvc,
            VisorSegmentationConfig seg, String inclProps, int[] inclEvtTypes, boolean restEnabled,
            String jettyPath, String jettyHost, Integer jettyPort, Map<String, ?> userAttrs,
            Iterable<VisorCacheConfig> caches, Map<String, String> env,
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
            this.restEnabled = restEnabled;
            this.jettyPath = jettyPath;
            this.jettyHost = jettyHost;
            this.jettyPort = jettyPort;
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
         * @return P 2 p.
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
         * @return Execute svc.
         */
        public VisorExecServiceConfig executeSvc() {
            return execSvc;
        }

        /**
         * @return Seg.
         */
        public VisorSegmentationConfig seg() {
            return seg;
        }

        /**
         * @return Incl properties.
         */
        public String inclProperties() {
            return inclProps;
        }

        /**
         * @return Incl event types.
         */
        public int[] inclEventTypes() {
            return inclEvtTypes;
        }

        /**
         * @return Rest enabled.
         */
        public boolean restEnabled() {
            return restEnabled;
        }

        /**
         * @return Jetty path.
         */
        public String jettyPath() {
            return jettyPath;
        }

        /**
         * @return Jetty host.
         */
        public String jettyHost() {
            return jettyHost;
        }

        /**
         * @return Jetty port.
         */
        public Integer jettyPort() {
            return jettyPort;
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
         * @return Env.
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
                c.isRestEnabled(),
                c.getRestJettyPath(),
                getProperty(GG_JETTY_HOST),
                intValue(GG_JETTY_PORT, null),
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
