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

package org.apache.ignite.internal.visor.node;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static java.lang.System.getProperty;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DAEMON;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_HOME;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOCAL_HOST;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_NO_ASCII;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_NO_DISCO_ORDER;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_NO_SHUTDOWN_HOOK;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PROG_NAME;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SUCCESS_FILE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.boolValue;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactObject;

/**
 * Data transfer object for node basic configuration properties.
 */
public class VisorBasicConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Grid name. */
    private String gridName;

    /** IGNITE_HOME determined at startup. */
    private String ggHome;

    /** Local host value used. */
    private String locHost;

    /** Node id. */
    private UUID nodeId;

    /** Marshaller used. */
    private String marsh;

    /** Deployment Mode. */
    private Object deployMode;

    /** Client mode flag. */
    private Boolean clientMode;

    /** Whether this node daemon or not. */
    private boolean daemon;

    /** Whether remote JMX is enabled. */
    private boolean jmxRemote;

    /** Is node restart enabled. */
    private boolean restart;

    /** Network timeout. */
    private long netTimeout;

    /** Logger used on node. */
    private String log;

    /** Discovery startup delay. */
    private long discoStartupDelay;

    /** MBean server name */
    private String mBeanSrv;

    /** Whether ASCII logo is disabled. */
    private boolean noAscii;

    /** Whether no discovery order is allowed. */
    private boolean noDiscoOrder;

    /** Whether shutdown hook is disabled. */
    private boolean noShutdownHook;

    /** Name of command line program. */
    private String progName;

    /** Whether node is in quiet mode. */
    private boolean quiet;

    /** Success file name. */
    private String successFile;

    /** Whether update checker is enabled. */
    private boolean updateNtf;

    /**
     * @param ignite Grid.
     * @param c Grid configuration.
     * @return Data transfer object for node basic configuration properties.
     */
    public static VisorBasicConfiguration from(IgniteEx ignite, IgniteConfiguration c) {
        VisorBasicConfiguration cfg = new VisorBasicConfiguration();

        cfg.gridName = c.getGridName();
        cfg.ggHome = getProperty(IGNITE_HOME, c.getIgniteHome());
        cfg.locHost = getProperty(IGNITE_LOCAL_HOST, c.getLocalHost());
        cfg.nodeId = ignite.localNode().id();
        cfg.marsh = compactClass(c.getMarshaller());
        cfg.deployMode = compactObject(c.getDeploymentMode());
        cfg.clientMode = c.isClientMode();
        cfg.daemon = boolValue(IGNITE_DAEMON, c.isDaemon());
        cfg.jmxRemote = ignite.isJmxRemoteEnabled();
        cfg.restart = ignite.isRestartEnabled();
        cfg.netTimeout = c.getNetworkTimeout();
        cfg.log = compactClass(c.getGridLogger());
        cfg.discoStartupDelay = c.getDiscoveryStartupDelay();
        cfg.mBeanSrv = compactClass(c.getMBeanServer());
        cfg.noAscii = boolValue(IGNITE_NO_ASCII, false);
        cfg.noDiscoOrder = boolValue(IGNITE_NO_DISCO_ORDER, false);
        cfg.noShutdownHook = boolValue(IGNITE_NO_SHUTDOWN_HOOK, false);
        cfg.progName = getProperty(IGNITE_PROG_NAME);
        cfg.quiet = boolValue(IGNITE_QUIET, true);
        cfg.successFile = getProperty(IGNITE_SUCCESS_FILE);
        cfg.updateNtf = boolValue(IGNITE_UPDATE_NOTIFIER, true);

        return cfg;
    }

    /**
     * @return Grid name.
     */
    @Nullable public String gridName() {
        return gridName;
    }

    /**
     * @return IGNITE_HOME determined at startup.
     */
    @Nullable public String ggHome() {
        return ggHome;
    }

    /**
     * @return Local host value used.
     */
    @Nullable public String localHost() {
        return locHost;
    }

    /**
     * @return Node id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Marshaller used.
     */
    public String marshaller() {
        return marsh;
    }

    /**
     * @return Deployment Mode.
     */
    public Object deploymentMode() {
        return deployMode;
    }

    /**
     * @return Client mode flag.
     */
    public Boolean clientMode() {
        return clientMode;
    }

    /**
     * @return Whether this node daemon or not.
     */
    public boolean daemon() {
        return daemon;
    }

    /**
     * @return Whether remote JMX is enabled.
     */
    public boolean jmxRemote() {
        return jmxRemote;
    }

    /**
     * @return Is node restart enabled.
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
     * @return Logger used on node.
     */
    public String logger() {
        return log;
    }

    /**
     * @return Discovery startup delay.
     */
    public long discoStartupDelay() {
        return discoStartupDelay;
    }

    /**
     * @return MBean server name
     */
    @Nullable public String mBeanServer() {
        return mBeanSrv;
    }

    /**
     * @return Whether ASCII logo is disabled.
     */
    public boolean noAscii() {
        return noAscii;
    }

    /**
     * @return Whether no discovery order is allowed.
     */
    public boolean noDiscoOrder() {
        return noDiscoOrder;
    }

    /**
     * @return Whether shutdown hook is disabled.
     */
    public boolean noShutdownHook() {
        return noShutdownHook;
    }

    /**
     * @return Name of command line program.
     */
    public String programName() {
        return progName;
    }

    /**
     * @return Whether node is in quiet mode.
     */
    public boolean quiet() {
        return quiet;
    }

    /**
     * @return Success file name.
     */
    public String successFile() {
        return successFile;
    }

    /**
     * @return Whether update checker is enabled.
     */
    public boolean updateNotifier() {
        return updateNtf;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorBasicConfiguration.class, this);
    }
}