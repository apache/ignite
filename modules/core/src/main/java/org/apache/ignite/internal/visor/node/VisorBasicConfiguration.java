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

import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static java.lang.System.*;
import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

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

    /** Whether this node daemon or not. */
    private boolean daemon;

    /** Whether remote JMX is enabled. */
    private boolean jmxRemote;

    /** Is node restart enabled. */
    private boolean restart;

    /** Network timeout. */
    private long netTimeout;

    /** Node license URL */
    private String licenseUrl;

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

    /** Security credentials. */
    private String securityCred;

    /**
     * @param g Grid.
     * @param c Grid configuration.
     * @return Data transfer object for node basic configuration properties.
     */
    public static VisorBasicConfiguration from(IgniteEx g, IgniteConfiguration c) {
        VisorBasicConfiguration cfg = new VisorBasicConfiguration();

        cfg.gridName(c.getGridName());
        cfg.ggHome(getProperty(IGNITE_HOME, c.getIgniteHome()));
        cfg.localHost(getProperty(IGNITE_LOCAL_HOST, c.getLocalHost()));
        cfg.nodeId(g.localNode().id());
        cfg.marshaller(compactClass(c.getMarshaller()));
        cfg.deploymentMode(compactObject(c.getDeploymentMode()));
        cfg.daemon(boolValue(IGNITE_DAEMON, c.isDaemon()));
        cfg.jmxRemote(g.isJmxRemoteEnabled());
        cfg.restart(g.isRestartEnabled());
        cfg.networkTimeout(c.getNetworkTimeout());
        cfg.licenseUrl(c.getLicenseUrl());
        cfg.logger(compactClass(c.getGridLogger()));
        cfg.discoStartupDelay(c.getDiscoveryStartupDelay());
        cfg.mBeanServer(compactClass(c.getMBeanServer()));
        cfg.noAscii(boolValue(IGNITE_NO_ASCII, false));
        cfg.noDiscoOrder(boolValue(IGNITE_NO_DISCO_ORDER, false));
        cfg.noShutdownHook(boolValue(IGNITE_NO_SHUTDOWN_HOOK, false));
        cfg.programName(getProperty(IGNITE_PROG_NAME));
        cfg.quiet(boolValue(IGNITE_QUIET, true));
        cfg.successFile(getProperty(IGNITE_SUCCESS_FILE));
        cfg.updateNotifier(boolValue(IGNITE_UPDATE_NOTIFIER, true));
        cfg.securityCredentialsProvider(compactClass(c.getSecurityCredentialsProvider()));

        return cfg;
    }

    /**
     * @return Grid name.
     */
    @Nullable public String gridName() {
        return gridName;
    }

    /**
     * @param gridName New grid name.
     */
    public void gridName(@Nullable String gridName) {
        this.gridName = gridName;
    }

    /**
     * @return IGNITE_HOME determined at startup.
     */
    @Nullable public String ggHome() {
        return ggHome;
    }

    /**
     * @param ggHome New IGNITE_HOME determined at startup.
     */
    public void ggHome(@Nullable String ggHome) {
        this.ggHome = ggHome;
    }

    /**
     * @return Local host value used.
     */
    @Nullable public String localHost() {
        return locHost;
    }

    /**
     * @param locHost New local host value used.
     */
    public void localHost(@Nullable String locHost) {
        this.locHost = locHost;
    }

    /**
     * @return Node id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId New node id.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Marshaller used.
     */
    public String marshaller() {
        return marsh;
    }

    /**
     * @param marsh New marshaller used.
     */
    public void marshaller(String marsh) {
        this.marsh = marsh;
    }

    /**
     * @return Deployment Mode.
     */
    public Object deploymentMode() {
        return deployMode;
    }

    /**
     * @param deployMode New Deployment Mode.
     */
    public void deploymentMode(Object deployMode) {
        this.deployMode = deployMode;
    }

    /**
     * @return Whether this node daemon or not.
     */
    public boolean daemon() {
        return daemon;
    }

    /**
     * @param daemon New whether this node daemon or not.
     */
    public void daemon(boolean daemon) {
        this.daemon = daemon;
    }

    /**
     * @return Whether remote JMX is enabled.
     */
    public boolean jmxRemote() {
        return jmxRemote;
    }

    /**
     * @param jmxRemote New whether remote JMX is enabled.
     */
    public void jmxRemote(boolean jmxRemote) {
        this.jmxRemote = jmxRemote;
    }

    /**
     * @return Is node restart enabled.
     */
    public boolean restart() {
        return restart;
    }

    /**
     * @param restart New is node restart enabled.
     */
    public void restart(boolean restart) {
        this.restart = restart;
    }

    /**
     * @return Network timeout.
     */
    public long networkTimeout() {
        return netTimeout;
    }

    /**
     * @param netTimeout New network timeout.
     */
    public void networkTimeout(long netTimeout) {
        this.netTimeout = netTimeout;
    }

    /**
     * @return Node license URL
     */
    @Nullable public String licenseUrl() {
        return licenseUrl;
    }

    /**
     * @param licenseUrl New node license URL
     */
    public void licenseUrl(@Nullable String licenseUrl) {
        this.licenseUrl = licenseUrl;
    }

    /**
     * @return Logger used on node.
     */
    public String logger() {
        return log;
    }

    /**
     * @param log New logger used on node.
     */
    public void logger(String log) {
        this.log = log;
    }

    /**
     * @return Discovery startup delay.
     */
    public long discoStartupDelay() {
        return discoStartupDelay;
    }

    /**
     * @param discoStartupDelay New discovery startup delay.
     */
    public void discoStartupDelay(long discoStartupDelay) {
        this.discoStartupDelay = discoStartupDelay;
    }

    /**
     * @return MBean server name
     */
    @Nullable public String mBeanServer() {
        return mBeanSrv;
    }

    /**
     * @param mBeanSrv New mBean server name
     */
    public void mBeanServer(@Nullable String mBeanSrv) {
        this.mBeanSrv = mBeanSrv;
    }

    /**
     * @return Whether ASCII logo is disabled.
     */
    public boolean noAscii() {
        return noAscii;
    }

    /**
     * @param noAscii New whether ASCII logo is disabled.
     */
    public void noAscii(boolean noAscii) {
        this.noAscii = noAscii;
    }

    /**
     * @return Whether no discovery order is allowed.
     */
    public boolean noDiscoOrder() {
        return noDiscoOrder;
    }

    /**
     * @param noDiscoOrder New whether no discovery order is allowed.
     */
    public void noDiscoOrder(boolean noDiscoOrder) {
        this.noDiscoOrder = noDiscoOrder;
    }

    /**
     * @return Whether shutdown hook is disabled.
     */
    public boolean noShutdownHook() {
        return noShutdownHook;
    }

    /**
     * @param noShutdownHook New whether shutdown hook is disabled.
     */
    public void noShutdownHook(boolean noShutdownHook) {
        this.noShutdownHook = noShutdownHook;
    }

    /**
     * @return Name of command line program.
     */
    public String programName() {
        return progName;
    }

    /**
     * @param progName New name of command line program.
     */
    public void programName(String progName) {
        this.progName = progName;
    }

    /**
     * @return Whether node is in quiet mode.
     */
    public boolean quiet() {
        return quiet;
    }

    /**
     * @param quiet New whether node is in quiet mode.
     */
    public void quiet(boolean quiet) {
        this.quiet = quiet;
    }

    /**
     * @return Success file name.
     */
    public String successFile() {
        return successFile;
    }

    /**
     * @param successFile New success file name.
     */
    public void successFile(String successFile) {
        this.successFile = successFile;
    }

    /**
     * @return Whether update checker is enabled.
     */
    public boolean updateNotifier() {
        return updateNtf;
    }

    /**
     * @param updateNtf New whether update checker is enabled.
     */
    public void updateNotifier(boolean updateNtf) {
        this.updateNtf = updateNtf;
    }

    /**
     * @return Security credentials.
     */
    @Nullable public String securityCredentialsProvider() {
        return securityCred;
    }

    /**
     * @param securityCred New security credentials.
     */
    public void securityCredentialsProvider(@Nullable String securityCred) {
        this.securityCred = securityCred;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorBasicConfiguration.class, this);
    }
}
