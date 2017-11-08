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

package org.apache.ignite.internal;

import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CONFIG_URL;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_NO_ASCII;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REST_START_ON_CLIENT;
import static org.apache.ignite.IgniteSystemProperties.snapshot;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.BUILD_TSTAMP_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.IgniteVersionUtils.REV_HASH_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;

/**
 *
 */
public class OutputVariousInformation {
    /**
     * @param log Logger.
     */
    OutputVariousInformation(IgniteLogger log, IgniteConfiguration cfg) {
        this.cfg = cfg;

        this.log = log;
    }

    /** Logger. */
    private IgniteLogger log;

    /** Configuration. */
    private IgniteConfiguration cfg;

    /** System line separator. */
    private static final String NL = U.nl();

    /** Ignite site that is shown in log messages. */
    private static final String SITE = "ignite.apache.org";

    /** */
    private String igniteInstanceName;

    /**
     * Acks ASCII-logo. Thanks to http://patorjk.com/software/taag
     */
    void ackAsciiLogo() {
        assert log != null;

        if (System.getProperty(IGNITE_NO_ASCII) == null) {
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
                        ">>> Ignite documentation: " + "http://" + SITE + NL
                );
            }

            if (log.isQuiet()) {
                U.quiet(false,
                        "   __________  ________________ ",
                        "  /  _/ ___/ |/ /  _/_  __/ __/ ",
                        " _/ // (7 7    // /  / / / _/   ",
                        "/___/\\___/_/|_/___/ /_/ /___/  ",
                        "",
                        ver,
                        COPYRIGHT,
                        "",
                        "Ignite documentation: " + "http://" + SITE,
                        "",
                        "Quiet mode.");

                String fileName = log.fileName();

                if (fileName != null)
                    U.quiet(false, "  ^-- Logging to file '" + fileName + '\'');

                U.quiet(false,
                        "  ^-- To see **FULL** console log here add -DIGNITE_QUIET=false or \"-v\" to ignite.{sh|bat}",
                        "");
            }
        }
    }

    /**
     * Acks configuration URL.
     */
    void ackConfigUrl() {
        assert log != null;

        if (log.isInfoEnabled())
            log.info("Config URL: " + System.getProperty(IGNITE_CONFIG_URL, "n/a"));
    }

    /**
     * Acks daemon mode status.
     */
    void ackDaemon(boolean isDaemon) {
        assert log != null;

        if (log.isInfoEnabled())
            log.info("Daemon mode: " + (isDaemon ? "on" : "off"));
    }

    /**
     * Logs out OS information.
     */
    void ackOsInfo() {
        assert log != null;

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
    void ackLanguageRuntime(String getLanguage) {
        assert log != null;

        if (log.isQuiet())
            U.quiet(false, "VM information: " + U.jdkString());

        if (log.isInfoEnabled()) {
            log.info("Language runtime: " + getLanguage);
            log.info("VM information: " + U.jdkString());
            log.info("VM total memory: " + U.heapSize(2) + "GB");
        }
    }

    /**
     * Acks remote management.
     */
    void ackRemoteManagement(boolean isJmxRemoteEnabled, boolean isRestartEnabled) {
        assert log != null;

        if (!log.isInfoEnabled())
            return;

        SB sb = new SB();

        sb.a("Remote Management [");

        sb.a("restart: ").a(onOff(isRestartEnabled)).a(", ");
        sb.a("REST: ").a(onOff(isRestEnabled())).a(", ");
        sb.a("JMX (");
        sb.a("remote: ").a(onOff(isJmxRemoteEnabled));

        if (isJmxRemoteEnabled) {
            sb.a(", ");

            sb.a("port: ").a(System.getProperty("com.sun.management.jmxremote.port", "<n/a>")).a(", ");
            sb.a("auth: ").a(onOff(Boolean.getBoolean("com.sun.management.jmxremote.authenticate"))).a(", ");

            // By default SSL is enabled, that's why additional check for null is needed.
            // See http://docs.oracle.com/javase/6/docs/technotes/guides/management/agent.html
            sb.a("ssl: ").a(onOff(Boolean.getBoolean("com.sun.management.jmxremote.ssl") ||
                    System.getProperty("com.sun.management.jmxremote.ssl") == null));
        }

        sb.a(")");

        sb.a(']');

        log.info(sb.toString());
    }

    /**
     * Prints out VM arguments and IGNITE_HOME in info mode.
     *
     * @param rtBean Java runtime bean.
     */
    void ackVmArguments(RuntimeMXBean rtBean) {
        assert log != null;

        // Ack IGNITE_HOME and VM arguments.
        if (log.isInfoEnabled() && S.INCLUDE_SENSITIVE) {
            log.info("IGNITE_HOME=" + cfg.getIgniteHome());
            log.info("VM arguments: " + rtBean.getInputArguments());
        }
    }

    /**
     * Prints out class paths in debug mode.
     *
     * @param rtBean Java runtime bean.
     */
    void ackClassPaths(RuntimeMXBean rtBean) {
        assert log != null;

        // Ack all class paths.
        if (log.isDebugEnabled()) {
            log.debug("Boot class path: " + rtBean.getBootClassPath());
            log.debug("Class path: " + rtBean.getClassPath());
            log.debug("Library path: " + rtBean.getLibraryPath());
        }
    }

    /**
     * Prints all system properties in debug mode.
     */
    void ackSystemProperties() {
        assert log != null;
        if (log.isDebugEnabled() && S.INCLUDE_SENSITIVE)
            for (Map.Entry<Object, Object> entry : snapshot().entrySet())
                log.debug("System property [" + entry.getKey() + '=' + entry.getValue() + ']');
    }

    /**
     * Prints all environment variables in debug mode.
     */
    void ackEnvironmentVariables() {
        assert log != null;

        if (log.isDebugEnabled())
            for (Map.Entry<?, ?> envVar : System.getenv().entrySet())
                log.debug("Environment variable [" + envVar.getKey() + '=' + envVar.getValue() + ']');
    }

    /**
     *
     */
    void ackMemoryConfiguration() {
        MemoryConfiguration memCfg = cfg.getMemoryConfiguration();

        if (memCfg == null)
            return;

        U.log(log, "System cache's MemoryPolicy size is configured to " +
                (memCfg.getSystemCacheInitialSize() / (1024 * 1024)) + " MB. " +
                "Use MemoryConfiguration.systemCacheMemorySize property to change the setting.");
    }

    /**
     *
     */
    void ackCacheConfiguration() {
        CacheConfiguration[] cacheCfgs = cfg.getCacheConfiguration();

        if (cacheCfgs == null || cacheCfgs.length == 0)
            U.warn(log, "Cache is not configured - in-memory data grid is off.");
        else {
            SB sb = new SB();

            HashMap<String, ArrayList<String>> memPlcNamesMapping = new HashMap<>();

            for (CacheConfiguration c : cacheCfgs) {
                String cacheName = U.maskName(c.getName());

                String memPlcName = c.getMemoryPolicyName();

                if (CU.isSystemCache(cacheName))
                    memPlcName = "sysMemPlc";
                else if (memPlcName == null && cfg.getMemoryConfiguration() != null)
                    memPlcName = cfg.getMemoryConfiguration().getDefaultMemoryPolicyName();

                if (!memPlcNamesMapping.containsKey(memPlcName))
                    memPlcNamesMapping.put(memPlcName, new ArrayList<String>());

                ArrayList<String> cacheNames = memPlcNamesMapping.get(memPlcName);

                cacheNames.add(cacheName);
            }

            for (Map.Entry<String, ArrayList<String>> e : memPlcNamesMapping.entrySet()) {
                sb.a("in '").a(e.getKey()).a("' memoryPolicy: [");

                for (String s : e.getValue())
                    sb.a("'").a(s).a("', ");

                sb.d(sb.length() - 2, sb.length()).a("], ");
            }

            U.log(log, "Configured caches [" + sb.d(sb.length() - 2, sb.length()).toString() + ']');
        }
    }

    /**
     *
     */
    void ackP2pConfiguration() {
        assert cfg != null;

        if (cfg.isPeerClassLoadingEnabled())
            U.warn(
                    log,
                    "Peer class loading is enabled (disable it in production for performance and " +
                            "deployment consistency reasons)",
                    "Peer class loading is enabled (disable it for better performance)"
            );
    }

    /**
     *
     */
    void ackRebalanceConfiguration() throws IgniteCheckedException {
        if (cfg.getSystemThreadPoolSize() <= cfg.getRebalanceThreadPoolSize())
            throw new IgniteCheckedException("Rebalance thread pool size exceed or equals System thread pool size. " +
                    "Change IgniteConfiguration.rebalanceThreadPoolSize property before next start.");

        if (cfg.getRebalanceThreadPoolSize() < 1)
            throw new IgniteCheckedException("Rebalance thread pool size minimal allowed value is 1. " +
                    "Change IgniteConfiguration.rebalanceThreadPoolSize property before next start.");

        for (CacheConfiguration ccfg : cfg.getCacheConfiguration()) {
            if (ccfg.getRebalanceBatchesPrefetchCount() < 1)
                throw new IgniteCheckedException("Rebalance batches prefetch count minimal allowed value is 1. " +
                        "Change CacheConfiguration.rebalanceBatchesPrefetchCount property before next start. " +
                        "[cache=" + ccfg.getName() + "]");
        }
    }

    /**
     * Prints all user attributes in info mode.
     */
    void logNodeUserAttributes() {
        assert log != null;

        if (log.isInfoEnabled())
            for (Map.Entry<?, ?> attr : cfg.getUserAttributes().entrySet())
                log.info("Local node user attribute [" + attr.getKey() + '=' + attr.getValue() + ']');
    }

    /**
     * Prints all configuration properties in info mode and SPIs in debug mode.
     */
    void ackSpis() {
        assert log != null;

        if (log.isDebugEnabled()) {
            log.debug("+-------------+");
            log.debug("START SPI LIST:");
            log.debug("+-------------+");
            log.debug("Grid checkpoint SPI     : " + Arrays.toString(cfg.getCheckpointSpi()));
            log.debug("Grid collision SPI      : " + cfg.getCollisionSpi());
            log.debug("Grid communication SPI  : " + cfg.getCommunicationSpi());
            log.debug("Grid deployment SPI     : " + cfg.getDeploymentSpi());
            log.debug("Grid discovery SPI      : " + cfg.getDiscoverySpi());
            log.debug("Grid event storage SPI  : " + cfg.getEventStorageSpi());
            log.debug("Grid failover SPI       : " + Arrays.toString(cfg.getFailoverSpi()));
            log.debug("Grid load balancing SPI : " + Arrays.toString(cfg.getLoadBalancingSpi()));
        }
    }

    /**
     * Prints security status.
     */
    void ackSecurity(GridKernalContextImpl ctx) {
        assert log != null;

        U.quietAndInfo(log, "Security status [authentication=" + onOff(ctx.security().enabled())
                + ", tls/ssl=" + onOff(ctx.config().getSslContextFactory() != null) + ']');
    }

    /**
     * Prints start info.
     *
     * @param rtBean Java runtime bean.
     */
    void ackStart(RuntimeMXBean rtBean, GridKernalContextImpl ctx) {
        igniteInstanceName = cfg.getIgniteInstanceName();

        ClusterNode locNode = ctx.cluster().get().localNode();

        if (log.isQuiet()) {
            U.quiet(false, "");
            U.quiet(false, "Ignite node started OK (id=" + U.id8(locNode.id()) +
                    (F.isEmpty(igniteInstanceName) ? "" : ", instance name=" + igniteInstanceName) + ')');
        }

        if (log.isInfoEnabled()) {
            log.info("");

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
                            ">>> VM name: " + rtBean.getName() + NL +
                            (igniteInstanceName == null ? "" : ">>> Ignite instance name: " + igniteInstanceName + NL) +
                            ">>> Local node [" +
                            "ID=" + locNode.id().toString().toUpperCase() +
                            ", order=" + locNode.order() + ", clientMode=" + ctx.clientNode() +
                            "]" + NL +
                            ">>> Local node addresses: " + U.addressesAsString(locNode) + NL +
                            ">>> Local ports: " + sb + NL;

            log.info(str);
        }
    }

    /**
     * Gets "on" or "off" string for given boolean value.
     *
     * @param b Boolean value to convert.
     * @return Result string.
     */
    private String onOff(boolean b) {
        return b ? "on" : "off";
    }

    /**
     * @return Whether or not REST is enabled.
     */
    private boolean isRestEnabled() {
        assert cfg != null;

        return cfg.getConnectorConfiguration() != null &&
                // By default rest processor doesn't start on client nodes.
                (!isClientNode() || (isClientNode() && IgniteSystemProperties.getBoolean(IGNITE_REST_START_ON_CLIENT)));
    }

    /**
     * @return {@code True} if node client or daemon otherwise {@code false}.
     */
    private boolean isClientNode() {
        return cfg.isClientMode() || cfg.isDaemon();
    }
}
