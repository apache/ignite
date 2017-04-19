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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
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

/**
 * Data transfer object for node basic configuration properties.
 */
public class VisorBasicConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Ignite instance name. */
    private String igniteInstanceName;

    /** IGNITE_HOME determined at startup. */
    private String ggHome;

    /** Local host value used. */
    private String locHost;

    /** Node id. */
    private UUID nodeId;

    /** Marshaller used. */
    private String marsh;

    /** Deployment Mode. */
    private DeploymentMode deployMode;

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
     * Default constructor.
     */
    public VisorBasicConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for node basic configuration properties.
     *
     * @param ignite Grid.
     * @param c Grid configuration.
     */
    public VisorBasicConfiguration(IgniteEx ignite, IgniteConfiguration c) {
        igniteInstanceName = c.getIgniteInstanceName();
        ggHome = getProperty(IGNITE_HOME, c.getIgniteHome());
        locHost = getProperty(IGNITE_LOCAL_HOST, c.getLocalHost());
        nodeId = ignite.localNode().id();
        marsh = compactClass(c.getMarshaller());
        deployMode = c.getDeploymentMode();
        clientMode = c.isClientMode();
        daemon = boolValue(IGNITE_DAEMON, c.isDaemon());
        jmxRemote = ignite.isJmxRemoteEnabled();
        restart = ignite.isRestartEnabled();
        netTimeout = c.getNetworkTimeout();
        log = compactClass(c.getGridLogger());
        discoStartupDelay = c.getDiscoveryStartupDelay();
        mBeanSrv = compactClass(c.getMBeanServer());
        noAscii = boolValue(IGNITE_NO_ASCII, false);
        noDiscoOrder = boolValue(IGNITE_NO_DISCO_ORDER, false);
        noShutdownHook = boolValue(IGNITE_NO_SHUTDOWN_HOOK, false);
        progName = getProperty(IGNITE_PROG_NAME);
        quiet = boolValue(IGNITE_QUIET, true);
        successFile = getProperty(IGNITE_SUCCESS_FILE);
        updateNtf = boolValue(IGNITE_UPDATE_NOTIFIER, true);
    }

    /**
     * @return Ignite instance name.
     */
    @Nullable public String getIgniteInstanceName() {
        return igniteInstanceName;
    }

    /**
     * @return IGNITE_HOME determined at startup.
     */
    @Nullable public String getGgHome() {
        return ggHome;
    }

    /**
     * @return Local host value used.
     */
    @Nullable public String getLocalHost() {
        return locHost;
    }

    /**
     * @return Node id.
     */
    public UUID getNodeId() {
        return nodeId;
    }

    /**
     * @return Marshaller used.
     */
    public String getMarshaller() {
        return marsh;
    }

    /**
     * @return Deployment Mode.
     */
    public Object getDeploymentMode() {
        return deployMode;
    }

    /**
     * @return Client mode flag.
     */
    public Boolean isClientMode() {
        return clientMode;
    }

    /**
     * @return Whether this node daemon or not.
     */
    public boolean isDaemon() {
        return daemon;
    }

    /**
     * @return Whether remote JMX is enabled.
     */
    public boolean isJmxRemote() {
        return jmxRemote;
    }

    /**
     * @return Is node restart enabled.
     */
    public boolean isRestart() {
        return restart;
    }

    /**
     * @return Network timeout.
     */
    public long getNetworkTimeout() {
        return netTimeout;
    }

    /**
     * @return Logger used on node.
     */
    public String getLogger() {
        return log;
    }

    /**
     * @return Discovery startup delay.
     */
    public long getDiscoStartupDelay() {
        return discoStartupDelay;
    }

    /**
     * @return MBean server name
     */
    @Nullable public String getMBeanServer() {
        return mBeanSrv;
    }

    /**
     * @return Whether ASCII logo is disabled.
     */
    public boolean isNoAscii() {
        return noAscii;
    }

    /**
     * @return Whether no discovery order is allowed.
     */
    public boolean isNoDiscoOrder() {
        return noDiscoOrder;
    }

    /**
     * @return Whether shutdown hook is disabled.
     */
    public boolean isNoShutdownHook() {
        return noShutdownHook;
    }

    /**
     * @return Name of command line program.
     */
    public String getProgramName() {
        return progName;
    }

    /**
     * @return Whether node is in quiet mode.
     */
    public boolean isQuiet() {
        return quiet;
    }

    /**
     * @return Success file name.
     */
    public String getSuccessFile() {
        return successFile;
    }

    /**
     * @return Whether update checker is enabled.
     */
    public boolean isUpdateNotifier() {
        return updateNtf;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, igniteInstanceName);
        U.writeString(out, ggHome);
        U.writeString(out, locHost);
        U.writeUuid(out, nodeId);
        U.writeString(out, marsh);
        U.writeEnum(out, deployMode);
        out.writeObject(clientMode);
        out.writeBoolean(daemon);
        out.writeBoolean(jmxRemote);
        out.writeBoolean(restart);
        out.writeLong(netTimeout);
        U.writeString(out, log);
        out.writeLong(discoStartupDelay);
        U.writeString(out, mBeanSrv);
        out.writeBoolean(noAscii);
        out.writeBoolean(noDiscoOrder);
        out.writeBoolean(noShutdownHook);
        U.writeString(out, progName);
        out.writeBoolean(quiet);
        U.writeString(out, successFile);
        out.writeBoolean(updateNtf);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        igniteInstanceName = U.readString(in);
        ggHome = U.readString(in);
        locHost = U.readString(in);
        nodeId = U.readUuid(in);
        marsh = U.readString(in);
        deployMode = DeploymentMode.fromOrdinal(in.readByte());
        clientMode = (Boolean)in.readObject();
        daemon = in.readBoolean();
        jmxRemote = in.readBoolean();
        restart = in.readBoolean();
        netTimeout = in.readLong();
        log = U.readString(in);
        discoStartupDelay = in.readLong();
        mBeanSrv = U.readString(in);
        noAscii = in.readBoolean();
        noDiscoOrder = in.readBoolean();
        noShutdownHook = in.readBoolean();
        progName = U.readString(in);
        quiet = in.readBoolean();
        successFile = U.readString(in);
        updateNtf = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorBasicConfiguration.class, this);
    }
}
