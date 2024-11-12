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

package org.apache.ignite.internal.plugin;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridLoggerProxy;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CONFIG_URL;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOG_CLASSPATH_CONTENT_ON_STARTUP;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_NO_ASCII;
import static org.apache.ignite.internal.IgniteKernal.DFLT_LOG_CLASSPATH_CONTENT_ON_STARTUP;
import static org.apache.ignite.internal.IgniteKernal.NL;
import static org.apache.ignite.internal.IgniteKernal.SITE;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.BUILD_TSTAMP_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.IgniteVersionUtils.REV_HASH_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;
import static org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager.INTERNAL_DATA_REGION_NAMES;
import static org.apache.ignite.internal.util.IgniteUtils.MB;

/**
 * Default implementation of Ignite information.
 */
public class IgniteLogInfoProviderImpl implements IgniteLogInfoProvider {
    /** Format of output metrics. */
    private final DecimalFormat decimalFormat = new DecimalFormat("#.##", DecimalFormatSymbols.getInstance(Locale.US));

    /** {@inheritDoc} */
    @Override public void ackKernalInited(IgniteLogger log, IgniteConfiguration cfg) {
        assert log != null;

        RuntimeMXBean rtBean = ManagementFactory.getRuntimeMXBean();

        ackAsciiLogo(log, cfg, rtBean);
        ackConfigUrl(log);
        ackConfiguration(log, cfg);
        ackOsInfo(log);
        ackLanguageRuntime(log, cfg);
        ackRemoteManagement(log, cfg);
        ackLogger(log);
        ackVmArguments(log, cfg, rtBean);
        ackClassPaths(log, rtBean);
        ackSystemProperties(log);
        ackEnvironmentVariables(log);
        ackMemoryConfiguration(log, cfg);
        ackCacheConfiguration(log, cfg);

        if (cfg.isPeerClassLoadingEnabled())
            ackP2pConfiguration(log);

        ackRebalanceConfiguration(log, cfg);
        ackIPv4StackFlagIsSet(log);
        ackWaitForBackupsOnShutdownPropertyIsUsed(log);
        ack3rdPartyLicenses(log, cfg);
        logNodeUserAttributes(log, cfg);
        ackSpis(log, cfg);
    }

    /** {@inheritDoc} */
    @Override public void ackNodeBasicMetrics(IgniteLogger log, Ignite ignite) {
        ackNodeBasicMetrics(log, (IgniteEx)ignite, decimalFormat);
    }

    /** {@inheritDoc} */
    @Override public void ackNodeDataStorageMetrics(IgniteLogger log, Ignite ignite) {
        GridKernalContext ctx = ((IgniteEx)ignite).context();

        dataStorageReport(log, ctx.cache().context().database(), decimalFormat);
    }

    /** {@inheritDoc} */
    @Override public void ackNodeMemoryStatisticsMetrics(IgniteLogger log, Ignite ignite) {
        GridKernalContext ctx = ((IgniteEx)ignite).context();

        memoryStatisticsReport(log, ctx.cache().context().database(), decimalFormat);
    }

    /** {@inheritDoc} */
    @Override public void ackKernalStarted(IgniteLogger log, Ignite ignite) {
        IgniteEx igEx = (IgniteEx)ignite;

        ackSecurity(log, ignite);
        ackPerformanceSuggestions(log, igEx);
        ackClassPathContent(log);
        ackNodeInfo(log, igEx);
    }

    /** {@inheritDoc} */
    @Override public void ackKernalStopped(IgniteLogger log, Ignite ignite, boolean err) {
        ackNodeStopped(log, (IgniteEx)ignite, err);
    }

    /**
     * Acks ASCII-logo. Thanks to http://patorjk.com/software/taag
     */
    void ackAsciiLogo(IgniteLogger log, IgniteConfiguration cfg, RuntimeMXBean rtBean) {
        if (System.getProperty(IGNITE_NO_ASCII) != null)
            return;

        String ver = "ver. " + ACK_VER_STR;

        // Big thanks to: http://patorjk.com/software/taag
        // Font name "Small Slant"
        if (log.isInfoEnabled()) {
            log.info(NL + NL +
                ">>>    __________  ________________  " + NL +
                ">>>   /  _/ ___/ |/ /  _/_  __/ __/  " + NL +
                ">>>  _/ // (7 7    // /  / / / _/    " + NL +
                ">>> /___/\\___/_/|_/___/ /_/ /___/   " + NL +
                ">>> " + NL +
                ">>> " + ver + NL +
                ">>> " + COPYRIGHT + NL +
                ">>> " + NL +
                ">>> Ignite documentation: " + "https://" + SITE + NL
            );
        }

        if (log.isQuiet()) {
            U.quiet(false,
                "   __________  ________________ ",
                "  /  _/ ___/ |/ /  _/_  __/ __/ ",
                " _/ // (7 7    // /  / / / _/   ",
                "/___/\\___/_/|_/___/ /_/ /x___/  ",
                "",
                ver,
                COPYRIGHT,
                "",
                "Ignite documentation: " + "https://" + SITE,
                "",
                "Quiet mode.");

            String fileName = log.fileName();

            if (fileName != null)
                U.quiet(false, "  ^-- Logging to file '" + fileName + '\'');

            U.quiet(false, "  ^-- Logging by '" + ((GridLoggerProxy)log).getLoggerInfo() + '\'');

            U.quiet(false,
                "  ^-- To see **FULL** console log here add -DIGNITE_QUIET=false or \"-v\" to ignite.{sh|bat}",
                "");
        }
    }

    /**
     * Acks configuration URL.
     */
    void ackConfigUrl(IgniteLogger log) {
        if (log.isInfoEnabled())
            log.info("Config URL: " + System.getProperty(IGNITE_CONFIG_URL, "n/a"));
    }

    /**
     * @param cfg Ignite configuration to ack.
     */
    void ackConfiguration(IgniteLogger log, IgniteConfiguration cfg) {
        if (log.isInfoEnabled())
            log.info(cfg.toString());
    }

    /**
     * Logs out OS information.
     */
    void ackOsInfo(IgniteLogger log) {
        if (log.isQuiet())
            U.quiet(false, "OS: " + U.osString());

        if (log.isInfoEnabled()) {
            log.info("OS: " + U.osString());
            log.info("OS user: " + System.getProperty("user.name"));

            int jvmPid = U.jvmPid();

            log.info("PID: " + (jvmPid == -1 ? "N/A" : jvmPid));
        }
    }

    /**
     * Logs out language runtime.
     */
    void ackLanguageRuntime(IgniteLogger log, IgniteConfiguration cfg) {
        if (log.isQuiet())
            U.quiet(false, "VM information: " + U.jdkString());

        if (log.isInfoEnabled()) {
            log.info("Language runtime: " + U.language(U.resolveClassLoader(cfg)));
            log.info("VM information: " + U.jdkString());
            log.info("VM total memory: " + U.heapSize(2) + "GB");
        }
    }

    /**
     * Acks remote management.
     */
    void ackRemoteManagement(IgniteLogger log, IgniteConfiguration cfg) {
        if (!log.isInfoEnabled())
            return;

        SB sb = new SB();

        sb.a("Remote Management [");

        boolean on = U.isJmxRemoteEnabled();

        sb.a("restart: ").a(onOff(U.isRestartEnabled())).a(", ");
        sb.a("REST: ").a(onOff(U.isRestEnabled(cfg))).a(", ");
        sb.a("JMX (");
        sb.a("remote: ").a(onOff(on));

        if (on) {
            sb.a(", ");

            sb.a("port: ").a(System.getProperty("com.sun.management.jmxremote.port", "<n/a>")).a(", ");
            sb.a("auth: ").a(onOff(Boolean.getBoolean("com.sun.management.jmxremote.authenticate"))).a(", ");

            // By default, SSL is enabled, that's why additional check for null is needed.
            // See https://docs.oracle.com/en/java/javase/11/management/monitoring-and-management-using-jmx-technology.html
            sb.a("ssl: ").a(onOff(Boolean.getBoolean("com.sun.management.jmxremote.ssl") ||
                System.getProperty("com.sun.management.jmxremote.ssl") == null));
        }

        sb.a(")");

        sb.a(']');

        log.info(sb.toString());
    }

    /**
     * Acks Logger configuration.
     */
    void ackLogger(IgniteLogger log) {
        if (log.isInfoEnabled())
            log.info("Logger: " + ((GridLoggerProxy)log).getLoggerInfo());
    }

    /**
     * Prints out VM arguments and IGNITE_HOME in info mode.
     *
     * @param rtBean Java runtime bean.
     */
    void ackVmArguments(IgniteLogger log, IgniteConfiguration cfg, RuntimeMXBean rtBean) {
        // Ack IGNITE_HOME and VM arguments.
        if (log.isInfoEnabled() && S.includeSensitive()) {
            log.info("IGNITE_HOME=" + cfg.getIgniteHome());
            log.info("VM arguments: " + rtBean.getInputArguments());
        }
    }

    /**
     * Prints out class paths in debug mode.
     *
     * @param rtBean Java runtime bean.
     */
    void ackClassPaths(IgniteLogger log, RuntimeMXBean rtBean) {
        // Ack all class paths.
        if (log.isDebugEnabled()) {
            try {
                log.debug("Boot class path: " + rtBean.getBootClassPath());
                log.debug("Class path: " + rtBean.getClassPath());
                log.debug("Library path: " + rtBean.getLibraryPath());
            }
            catch (Exception ignore) {
                // No-op: ignore for Java 9+ and non-standard JVMs.
            }
        }
    }

    /**
     * Prints all system properties in debug mode.
     */
    void ackSystemProperties(IgniteLogger log) {
        if (log.isDebugEnabled() && S.includeSensitive())
            for (Map.Entry<Object, Object> entry : IgniteSystemProperties.snapshot().entrySet())
                log.debug("System property [" + entry.getKey() + '=' + entry.getValue() + ']');
    }

    /**
     * Prints all environment variables in debug mode.
     */
    void ackEnvironmentVariables(IgniteLogger log) {
        if (log.isDebugEnabled())
            for (Map.Entry<?, ?> envVar : System.getenv().entrySet())
                log.debug("Environment variable [" + envVar.getKey() + '=' + envVar.getValue() + ']');
    }

    /**
     * Acknowledge the Ignite configuration related to the data storage.
     */
    void ackMemoryConfiguration(IgniteLogger log, IgniteConfiguration cfg) {
        DataStorageConfiguration memCfg = cfg.getDataStorageConfiguration();

        if (memCfg == null)
            return;

        U.log(log, "System cache's DataRegion size is configured to " +
            (memCfg.getSystemDataRegionConfiguration().getInitialSize() / (1024 * 1024)) + " MB. " +
            "Use DataStorageConfiguration.systemRegionInitialSize property to change the setting.");
    }

    /**
     * Acknowledge all caches configurations presented in the IgniteConfiguration.
     */
    void ackCacheConfiguration(IgniteLogger log, IgniteConfiguration cfg) {
        CacheConfiguration[] cacheCfgs = cfg.getCacheConfiguration();

        if (cacheCfgs == null || cacheCfgs.length == 0)
            U.warn(log, "Cache is not configured - in-memory data grid is off.");
        else {
            SB sb = new SB();

            HashMap<String, ArrayList<String>> memPlcNamesMapping = new HashMap<>();

            for (CacheConfiguration c : cacheCfgs) {
                String cacheName = U.maskName(c.getName());

                String memPlcName = c.getDataRegionName();

                if (CU.isSystemCache(cacheName))
                    memPlcName = "sysMemPlc";
                else if (memPlcName == null && cfg.getDataStorageConfiguration() != null)
                    memPlcName = cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().getName();

                if (!memPlcNamesMapping.containsKey(memPlcName))
                    memPlcNamesMapping.put(memPlcName, new ArrayList<String>());

                ArrayList<String> cacheNames = memPlcNamesMapping.get(memPlcName);

                cacheNames.add(cacheName);
            }

            for (Map.Entry<String, ArrayList<String>> e : memPlcNamesMapping.entrySet()) {
                sb.a("in '").a(e.getKey()).a("' dataRegion: [");

                for (String s : e.getValue())
                    sb.a("'").a(s).a("', ");

                sb.d(sb.length() - 2, sb.length()).a("], ");
            }

            U.log(log, "Configured caches [" + sb.d(sb.length() - 2, sb.length()).toString() + ']');
        }
    }

    /**
     * Acknowledge configuration related to the peer class loading.
     */
    void ackP2pConfiguration(IgniteLogger log) {
        U.warn(log,
            "Peer class loading is enabled (disable it in production for performance and " +
                "deployment consistency reasons)");
    }

    /**
     * Acknowledge that the rebalance configuration properties are setted correctly.
     */
    void ackRebalanceConfiguration(IgniteLogger log, IgniteConfiguration cfg) {
        if (cfg.isClientMode()) {
            if (cfg.getRebalanceThreadPoolSize() != IgniteConfiguration.DFLT_REBALANCE_THREAD_POOL_SIZE)
                U.warn(log, "Setting the rebalance pool size has no effect on the client mode");
        }
        else {
            if (cfg.getRebalanceThreadPoolSize() < 1)
                throw new IgniteException("Rebalance thread pool size minimal allowed value is 1. " +
                    "Change IgniteConfiguration.rebalanceThreadPoolSize property before next start.");

            if (cfg.getRebalanceBatchesPrefetchCount() < 1)
                throw new IgniteException("Rebalance batches prefetch count minimal allowed value is 1. " +
                    "Change IgniteConfiguration.rebalanceBatchesPrefetchCount property before next start.");

            if (cfg.getRebalanceBatchSize() <= 0)
                throw new IgniteException("Rebalance batch size must be greater than zero. " +
                    "Change IgniteConfiguration.rebalanceBatchSize property before next start.");

            if (cfg.getRebalanceThrottle() < 0)
                throw new IgniteException("Rebalance throttle can't have negative value. " +
                    "Change IgniteConfiguration.rebalanceThrottle property before next start.");

            if (cfg.getRebalanceTimeout() < 0)
                throw new IgniteException("Rebalance message timeout can't have negative value. " +
                    "Change IgniteConfiguration.rebalanceTimeout property before next start.");

            for (CacheConfiguration ccfg : cfg.getCacheConfiguration()) {
                if (ccfg.getRebalanceBatchesPrefetchCount() < 1)
                    throw new IgniteException("Rebalance batches prefetch count minimal allowed value is 1. " +
                        "Change CacheConfiguration.rebalanceBatchesPrefetchCount property before next start. " +
                        "[cache=" + ccfg.getName() + "]");
            }
        }
    }

    /**
     * Prints warning if 'java.net.preferIPv4Stack=true' is not set.
     */
    void ackIPv4StackFlagIsSet(IgniteLogger log) {
        boolean preferIPv4 = Boolean.parseBoolean(System.getProperty("java.net.preferIPv4Stack"));

        if (!preferIPv4) {
            U.quietAndWarn(log, "Please set system property '-Djava.net.preferIPv4Stack=true' " +
                "to avoid possible problems in mixed environments.");
        }
    }

    /**
     * Prints warning if IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN is used.
     */
    void ackWaitForBackupsOnShutdownPropertyIsUsed(IgniteLogger log) {
        if (IgniteSystemProperties.getString(IgniteSystemProperties.IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN) == null)
            return;

        log.warning("IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN system property is deprecated and will be removed " +
            "in a future version. Use ShutdownPolicy instead.");
    }

    /**
     * Print 3-rd party licenses location.
     */
    void ack3rdPartyLicenses(IgniteLogger log, IgniteConfiguration cfg) {
        // Ack 3-rd party licenses location.
        if (log.isInfoEnabled() && cfg.getIgniteHome() != null)
            log.info("3-rd party licenses can be found at: " + cfg.getIgniteHome() + File.separatorChar + "libs" +
                File.separatorChar + "licenses");
    }

    /**
     * Prints all user attributes in info mode.
     */
    void logNodeUserAttributes(IgniteLogger log, IgniteConfiguration cfg) {
        if (log.isInfoEnabled())
            for (Map.Entry<?, ?> attr : cfg.getUserAttributes().entrySet())
                log.info("Local node user attribute [" + attr.getKey() + '=' + attr.getValue() + ']');
    }

    /**
     * Prints all configuration properties in info mode and SPIs in debug mode.
     */
    void ackSpis(IgniteLogger log, IgniteConfiguration cfg) {
        if (log.isDebugEnabled()) {
            log.debug("+-------------+");
            log.debug("START SPI LIST:");
            log.debug("+-------------+");
            log.debug("Grid checkpoint SPI       : " + Arrays.toString(cfg.getCheckpointSpi()));
            log.debug("Grid collision SPI        : " + cfg.getCollisionSpi());
            log.debug("Grid communication SPI    : " + cfg.getCommunicationSpi());
            log.debug("Grid deployment SPI       : " + cfg.getDeploymentSpi());
            log.debug("Grid discovery SPI        : " + cfg.getDiscoverySpi());
            log.debug("Grid event storage SPI    : " + cfg.getEventStorageSpi());
            log.debug("Grid failover SPI         : " + Arrays.toString(cfg.getFailoverSpi()));
            log.debug("Grid load balancing SPI   : " + Arrays.toString(cfg.getLoadBalancingSpi()));
            log.debug("Grid Metrics Exporter SPI : " + Arrays.toString(cfg.getMetricExporterSpi()));
        }
    }

    /**
     * Print performance suggestions.
     */
    void ackPerformanceSuggestions(IgniteLogger log, IgniteEx ignite) {
        GridKernalContext ctx = ignite.context();

        ctx.performance().add("Disable assertions (remove '-ea' from JVM options)", !U.assertionsEnabled());
        ctx.performance().logSuggestions(log, ignite.name());
    }

    /**
     * Prints the list of {@code *.jar} and {@code *.class} files containing in the classpath.
     */
    void ackClassPathContent(IgniteLogger log) {
        if (IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_QUIET, true))
            return;

        boolean enabled = IgniteSystemProperties.getBoolean(IGNITE_LOG_CLASSPATH_CONTENT_ON_STARTUP,
            DFLT_LOG_CLASSPATH_CONTENT_ON_STARTUP);

        if (enabled) {
            String clsPath = System.getProperty("java.class.path", ".");

            String[] clsPathElements = clsPath.split(File.pathSeparator);

            U.log(log, "Classpath value: " + clsPath);

            SB clsPathContent = new SB("List of files containing in classpath: ");

            for (String clsPathEntry : clsPathElements) {
                try {
                    if (clsPathEntry.contains("*"))
                        ackClassPathWildCard(clsPathEntry, clsPathContent);
                    else
                        ackClassPathEntry(clsPathEntry, clsPathContent);
                }
                catch (Exception e) {
                    U.warn(log, String.format("Could not log class path entry '%s': %s", clsPathEntry, e.getMessage()));
                }
            }

            U.log(log, clsPathContent.toString());
        }
    }

    /**
     * @param clsPathEntry Classpath string to process.
     * @param clsPathContent StringBuilder to attach path to.
     */
    void ackClassPathWildCard(String clsPathEntry, SB clsPathContent) {
        final int lastSeparatorIdx = clsPathEntry.lastIndexOf(File.separator);

        final int asteriskIdx = clsPathEntry.indexOf('*');

        // Just to log possibly incorrect entries to err.
        if (asteriskIdx >= 0 && asteriskIdx < lastSeparatorIdx)
            throw new RuntimeException("Could not parse classpath entry");

        final int fileMaskFirstIdx = lastSeparatorIdx + 1;

        final String fileMask =
            (fileMaskFirstIdx >= clsPathEntry.length()) ? "*.jar" : clsPathEntry.substring(fileMaskFirstIdx);

        Path path = Paths.get(lastSeparatorIdx > 0 ? clsPathEntry.substring(0, lastSeparatorIdx) : ".")
            .toAbsolutePath()
            .normalize();

        if (lastSeparatorIdx == 0)
            path = path.getRoot();

        try {
            DirectoryStream<Path> files =
                Files.newDirectoryStream(path, fileMask);

            for (Path f : files) {
                String s = f.toString();

                if (s.toLowerCase().endsWith(".jar"))
                    clsPathContent.a(f.toString()).a(";");
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * @param clsPathEntry Classpath string to process.
     * @param clsPathContent StringBuilder to attach path to.
     */
    void ackClassPathEntry(String clsPathEntry, SB clsPathContent) {
        File clsPathElementFile = new File(clsPathEntry);

        if (clsPathElementFile.isDirectory())
            clsPathContent.a(clsPathEntry).a(";");
        else {
            String extension = clsPathEntry.length() >= 4
                ? clsPathEntry.substring(clsPathEntry.length() - 4).toLowerCase()
                : null;

            if (".jar".equals(extension) || ".zip".equals(extension))
                clsPathContent.a(clsPathEntry).a(";");
        }
    }

    /**
     * Print local node information after kernal startup.
     */
    void ackNodeInfo(IgniteLogger log, IgniteEx ignite) {
        ClusterNode locNode = ignite.localNode();
        GridKernalContext ctx = ignite.context();

        if (log.isQuiet()) {
            U.quiet(false, "");

            U.quiet(false, "Ignite node started OK (id=" + U.id8(locNode.id()) +
                (F.isEmpty(ignite.name()) ? "" : ", instance name=" + ignite.name()) + ')');
        }

        if (log.isInfoEnabled()) {
            String ack = "Ignite ver. " + VER_STR + '#' + BUILD_TSTAMP_STR + "-sha1:" + REV_HASH_STR;

            String dash = U.dash(ack.length());

            SB sb = new SB();

            for (GridPortRecord rec : ctx.ports().records())
                sb.a(rec.protocol()).a(":").a(rec.port()).a(" ");

            String str =
                NL + NL +
                    ">>> " + dash + NL +
                    ">>> " + ack + NL +
                    ">>> " + dash + NL +
                    ">>> OS name: " + U.osString() + NL +
                    ">>> CPU(s): " + locNode.metrics().getTotalCpus() + NL +
                    ">>> Heap: " + U.heapSize(locNode, 2) + "GB" + NL +
                    ">>> VM name: " + ((IgniteKernal)ignite).vmName() + NL +
                    (ignite.name() == null ? "" : ">>> Ignite instance name: " + ignite.name() + NL) +
                    ">>> Local node [" +
                    "ID=" + locNode.id().toString().toUpperCase() +
                    ", order=" + locNode.order() + ", clientMode=" + ctx.clientNode() +
                    "]" + NL +
                    ">>> Local node addresses: " + U.addressesAsString(locNode) + NL +
                    ">>> Local ports: " + sb + NL +
                    ">>> " + dash + NL;

            log.info(str);
        }

        if (ctx.state().clusterState().state() == ClusterState.INACTIVE) {
            U.quietAndInfo(log, ">>> Ignite cluster is in INACTIVE state (limited functionality available). " +
                "Use control.(sh|bat) script or IgniteCluster.state(ClusterState.ACTIVE) to change the state.");
        }
    }

    /**
     * Logs out node metrics.
     */
    void ackNodeBasicMetrics(IgniteLogger log, IgniteEx ignite, DecimalFormat dblFmt) {
        if (!log.isInfoEnabled())
            return;

        GridKernalContext ctx = ignite.context();
        IgniteConfiguration cfg = ignite.configuration();

        ExecutorService execSvc = ctx.pools().getExecutorService();
        ExecutorService sysExecSvc = ctx.pools().getSystemExecutorService();
        ExecutorService stripedExecSvc = ctx.pools().getStripedExecutorService();
        Map<String, ? extends ExecutorService> customExecSvcs = ctx.pools().customExecutors();

        ClusterMetrics m = ignite.cluster().localNode().metrics();

        int locCpus = m.getTotalCpus();
        double cpuLoadPct = m.getCurrentCpuLoad() * 100;
        double avgCpuLoadPct = m.getAverageCpuLoad() * 100;
        double gcPct = m.getCurrentGcCpuLoad() * 100;

        // Heap params.
        long heapUsed = m.getHeapMemoryUsed();
        long heapMax = m.getHeapMemoryMaximum();

        long heapUsedInMBytes = heapUsed / MB;
        long heapCommInMBytes = m.getHeapMemoryCommitted() / MB;

        double freeHeapPct = heapMax > 0 ? ((double)((heapMax - heapUsed) * 100)) / heapMax : -1;

        int hosts = 0;
        int servers = 0;
        int clients = 0;
        int cpus = 0;

        try {
            ClusterMetrics metrics = ignite.cluster().metrics();

            Collection<ClusterNode> nodes0 = ignite.cluster().nodes();

            hosts = U.neighborhood(nodes0).size();
            servers = ignite.cluster().forServers().nodes().size();
            clients = ignite.cluster().forClients().nodes().size();
            cpus = metrics.getTotalCpus();
        }
        catch (IgniteException ignore) {
            // No-op.
        }

        String id = U.id8(ignite.localNode().id());

        AffinityTopologyVersion topVer = ctx.discovery().topologyVersionEx();

        ClusterNode locNode = ctx.discovery().localNode();

        String netDetails = "";

        if (!F.isEmpty(cfg.getLocalHost()))
            netDetails += ", localHost=" + cfg.getLocalHost();

        if (locNode instanceof TcpDiscoveryNode)
            netDetails += ", discoPort=" + ((TcpDiscoveryNode)locNode).discoveryPort();

        if (cfg.getCommunicationSpi() instanceof TcpCommunicationSpi)
            netDetails += ", commPort=" + ((TcpCommunicationSpi)cfg.getCommunicationSpi()).boundPort();

        SB msg = new SB();

        msg.nl()
            .a("Metrics for local node (to disable set 'metricsLogFrequency' to 0)").nl()
            .a("    ^-- Node [id=").a(id).a(ignite.name() != null ? ", name=" + ignite.name() : "").a(", uptime=")
            .a(((IgniteKernal)ignite).upTimeFormatted()).a("]").nl()
            .a("    ^-- Cluster [hosts=").a(hosts).a(", CPUs=").a(cpus).a(", servers=").a(servers)
            .a(", clients=").a(clients).a(", topVer=").a(topVer.topologyVersion())
            .a(", minorTopVer=").a(topVer.minorTopologyVersion()).a("]").nl()
            .a("    ^-- Network [addrs=").a(locNode.addresses()).a(netDetails).a("]").nl()
            .a("    ^-- CPU [CPUs=").a(locCpus).a(", curLoad=").a(dblFmt.format(cpuLoadPct))
            .a("%, avgLoad=").a(dblFmt.format(avgCpuLoadPct)).a("%, GC=").a(dblFmt.format(gcPct)).a("%]").nl()
            .a("    ^-- Heap [used=").a(dblFmt.format(heapUsedInMBytes))
            .a("MB, free=").a(dblFmt.format(freeHeapPct))
            .a("%, comm=").a(dblFmt.format(heapCommInMBytes)).a("MB]").nl()
            .a("    ^-- Outbound messages queue [size=").a(m.getOutboundMessagesQueueSize()).a("]").nl()
            .a("    ^-- ").a(createExecutorDescription("Public thread pool", execSvc)).nl()
            .a("    ^-- ").a(createExecutorDescription("System thread pool", sysExecSvc)).nl()
            .a("    ^-- ").a(createExecutorDescription("Striped thread pool", stripedExecSvc));

        if (customExecSvcs != null) {
            for (Map.Entry<String, ? extends ExecutorService> entry : customExecSvcs.entrySet())
                msg.nl().a("    ^-- ").a(createExecutorDescription(entry.getKey(), entry.getValue()));
        }

        log.info(msg.toString());

        ctx.cache().context().database().dumpStatistics(log);
    }

    /**
     * Create description of an executor service for logging.
     *
     * @param execSvcName Name of the service.
     * @param execSvc Service to create a description for.
     */
    String createExecutorDescription(String execSvcName, ExecutorService execSvc) {
        int poolSize = 0;
        int poolActiveThreads = 0;
        int poolQSize = 0;

        if (execSvc instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor exec = (ThreadPoolExecutor)execSvc;

            poolSize = exec.getPoolSize();
            poolActiveThreads = Math.min(poolSize, exec.getActiveCount());
            poolQSize = exec.getQueue().size();
        }
        else if (execSvc instanceof StripedExecutor) {
            StripedExecutor exec = (StripedExecutor)execSvc;

            poolSize = exec.stripesCount();
            poolActiveThreads = exec.activeStripesCount();
            poolQSize = exec.queueSize();
        }

        int poolIdleThreads = poolSize - poolActiveThreads;

        return execSvcName + " [active=" + poolActiveThreads + ", idle=" + poolIdleThreads + ", qSize=" + poolQSize + "]";
    }

    /**
     * Print data storage statistics.
     */
    void dataStorageReport(IgniteLogger log, IgniteCacheDatabaseSharedManager db, DecimalFormat dblFmt) {
        if (F.isEmpty(db.dataRegions()))
            return;

        SB dataRegionsInfo = new SB();
        dataRegionsInfo.nl();

        for (DataRegion region : db.dataRegions()) {
            DataRegionConfiguration regCfg = region.config();

            long pagesCnt = region.pageMemory().loadedPages();

            long offHeapUsed = region.pageMemory().systemPageSize() * pagesCnt;
            long offHeapInit = regCfg.getInitialSize();
            long offHeapMax = regCfg.getMaxSize();
            long offHeapComm = region.metrics().getOffHeapSize();

            long offHeapUsedInMBytes = offHeapUsed / MB;
            long offHeapMaxInMBytes = offHeapMax / MB;
            long offHeapCommInMBytes = offHeapComm / MB;
            long offHeapInitInMBytes = offHeapInit / MB;

            double freeOffHeapPct = offHeapMax > 0 ?
                ((double)((offHeapMax - offHeapUsed) * 100)) / offHeapMax : -1;

            String type = "user";

            try {
                if (region == db.dataRegion(null))
                    type = "default";
                else if (INTERNAL_DATA_REGION_NAMES.contains(regCfg.getName()))
                    type = "internal";
            }
            catch (IgniteCheckedException ice) {
                // Should never happen
                ice.printStackTrace();
            }

            dataRegionsInfo.a("    ^--   ")
                .a(regCfg.getName()).a(" region [type=").a(type)
                .a(", persistence=").a(regCfg.isPersistenceEnabled())
                .a(", lazyAlloc=").a(regCfg.isLazyMemoryAllocation()).a(',').nl()
                .a("      ...  ")
                .a("initCfg=").a(dblFmt.format(offHeapInitInMBytes))
                .a("MB, maxCfg=").a(dblFmt.format(offHeapMaxInMBytes))
                .a("MB, usedRam=").a(dblFmt.format(offHeapUsedInMBytes))
                .a("MB, freeRam=").a(dblFmt.format(freeOffHeapPct))
                .a("%, allocRam=").a(dblFmt.format(offHeapCommInMBytes)).a("MB");

            if (regCfg.isPersistenceEnabled()) {
                dataRegionsInfo.a(", allocTotal=")
                    .a(dblFmt.format(region.metrics().getTotalAllocatedSize() / MB)).a("MB");
            }

            dataRegionsInfo.a(']').nl();
        }

        if (log.isQuiet())
            U.quietMultipleLines(false, dataRegionsInfo.toString());
        else if (log.isInfoEnabled())
            log.info(dataRegionsInfo.toString());
    }

    /**
     * @param log Ignite Logger.
     * @param db Database Shared Manager.
     * @param dblFmt Double format.
     */
    void memoryStatisticsReport(IgniteLogger log, IgniteCacheDatabaseSharedManager db, DecimalFormat dblFmt) {
        if (F.isEmpty(db.dataRegions()))
            return;

        SB sb = new SB();
        sb.nl();

        long loadedPages = 0;
        long offHeapUsedSummary = 0;
        long offHeapMaxSummary = 0;
        long offHeapCommSummary = 0;
        long pdsUsedSummary = 0;
        boolean persistenceEnabled = false;

        for (DataRegion region : db.dataRegions()) {
            DataRegionConfiguration regCfg = region.config();

            long pagesCnt = region.pageMemory().loadedPages();

            long offHeapUsed = region.pageMemory().systemPageSize() * pagesCnt;
            long offHeapMax = regCfg.getMaxSize();
            long offHeapComm = region.metrics().getOffHeapSize();

            offHeapUsedSummary += offHeapUsed;
            offHeapMaxSummary += offHeapMax;
            offHeapCommSummary += offHeapComm;
            loadedPages += pagesCnt;

            if (regCfg.isPersistenceEnabled()) {
                pdsUsedSummary += region.metrics().getTotalAllocatedSize();

                persistenceEnabled = true;
            }
        }

        double freeOffHeapPct = offHeapMaxSummary > 0 ?
            ((double)((offHeapMaxSummary - offHeapUsedSummary) * 100)) / offHeapMaxSummary : -1;

        sb.nl()
            .a("Data storage metrics for local node (to disable set 'metricsLogFrequency' to 0)").nl()
            .a("    ^-- Off-heap memory [used=").a(dblFmt.format(offHeapUsedSummary / MB))
            .a("MB, free=").a(dblFmt.format(freeOffHeapPct))
            .a("%, allocated=").a(dblFmt.format(offHeapCommSummary / MB)).a("MB]").nl()
            .a("    ^-- Page memory [pages=").a(loadedPages).a("]").nl();

        if (persistenceEnabled)
            sb.a("    ^-- Ignite persistence [used=").a(dblFmt.format(pdsUsedSummary / MB)).a("MB]").nl();

        if (log.isQuiet())
            U.quietMultipleLines(false, sb.toString());
        else if (log.isInfoEnabled())
            log.info(sb.toString());
    }

    /** */
    void ackNodeStopped(IgniteLogger log, IgniteEx ignite, boolean err) {
        String igniteInstanceName = ignite.name();

        // Ack stop.
        if (log.isQuiet()) {
            String nodeName = igniteInstanceName == null ? "" : "name=" + igniteInstanceName + ", ";

            if (!err)
                U.quiet(false, "Ignite node stopped OK [" + nodeName + "uptime=" +
                    ((IgniteKernal)ignite).upTimeFormatted() + ']');
            else
                U.quiet(true, "Ignite node stopped wih ERRORS [" + nodeName + "uptime=" +
                    ((IgniteKernal)ignite).upTimeFormatted() + ']');
        }

        if (!log.isInfoEnabled())
            return;

        if (!err) {
            String ack = "Ignite ver. " + VER_STR + '#' + BUILD_TSTAMP_STR + "-sha1:" + REV_HASH_STR +
                " stopped OK";

            String dash = U.dash(ack.length());

            log.info(NL + NL +
                ">>> " + dash + NL +
                ">>> " + ack + NL +
                ">>> " + dash + NL +
                (igniteInstanceName == null ? "" : ">>> Ignite instance name: " + igniteInstanceName + NL) +
                ">>> Grid uptime: " + ((IgniteKernal)ignite).upTimeFormatted() +
                NL +
                NL);
        }
        else {
            String ack = "Ignite ver. " + VER_STR + '#' + BUILD_TSTAMP_STR + "-sha1:" + REV_HASH_STR +
                " stopped with ERRORS";

            String dash = U.dash(ack.length());

            log.info(NL + NL +
                ">>> " + ack + NL +
                ">>> " + dash + NL +
                (igniteInstanceName == null ? "" : ">>> Ignite instance name: " + igniteInstanceName + NL) +
                ">>> Grid uptime: " + ((IgniteKernal)ignite).upTimeFormatted() +
                NL +
                ">>> See log above for detailed error message." + NL +
                ">>> Note that some errors during stop can prevent grid from" + NL +
                ">>> maintaining correct topology since this node may have" + NL +
                ">>> not exited grid properly." + NL +
                NL);
        }
    }

    /**
     * Prints security status.
     */
    void ackSecurity(IgniteLogger log, Ignite ignite) {
        assert log != null;

        GridKernalContext ctx = ((IgniteEx)ignite).context();

        U.quietAndInfo(log, "Security status [authentication=" + onOff(ctx.security().enabled())
            + ", sandbox=" + onOff(ctx.security().sandbox().enabled())
            + ", tls/ssl=" + onOff(ctx.config().getSslContextFactory() != null) + ']');
    }

    /**
     * Gets "on" or "off" string for given boolean value.
     *
     * @param b Boolean value to convert.
     * @return Result string.
     */
    public static String onOff(boolean b) {
        return b ? "on" : "off";
    }
}
