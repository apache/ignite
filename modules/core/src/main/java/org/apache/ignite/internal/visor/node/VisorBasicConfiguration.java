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

    /** Active on start flag. */
    private boolean activeOnStart;

    /** Address resolver. */
    private String addrRslvr;

    /** Flag indicating whether cache sanity check is enabled. */
    private boolean cacheSanityCheckEnabled;

    /** User's class loader. */
    private String clsLdr;

    /** Consistent globally unique node ID which survives node restarts. */
    private String consistentId;

    /** Failure detection timeout. */
    private Long failureDetectionTimeout;

    /** Ignite work folder. */
    private String igniteWorkDir;

    /** */
    private boolean lateAffAssignment;

    /** Marshal local jobs. */
    private boolean marshLocJobs;

    /** Full metrics enabled flag. */
    private long metricsUpdateFreq;

    /** Failure detection timeout for client nodes. */
    private Long clientFailureDetectionTimeout;

    /** Message send retries delay. */
    private int sndRetryCnt;

    /** Interval between message send retries. */
    private long sndRetryDelay;

    /** Base port number for time server. */
    private int timeSrvPortBase;

    /** Port number range for time server. */
    private int timeSrvPortRange;

    /** Utility cache pool keep alive time. */
    private long utilityCacheKeepAliveTime;

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
        activeOnStart = c.isActiveOnStart();
        addrRslvr = compactClass(c.getAddressResolver());
        cacheSanityCheckEnabled = c.isCacheSanityCheckEnabled();
        clsLdr = compactClass(c.getClassLoader());
        consistentId = c.getConsistentId() != null ? String.valueOf(c.getConsistentId()) : null;
        failureDetectionTimeout = c.getFailureDetectionTimeout();
        igniteWorkDir = c.getWorkDirectory();
        lateAffAssignment = c.isLateAffinityAssignment();
        marshLocJobs = c.isMarshalLocalJobs();
        metricsUpdateFreq = c.getMetricsUpdateFrequency();
        clientFailureDetectionTimeout = c.getClientFailureDetectionTimeout();
        sndRetryCnt = c.getNetworkSendRetryCount();
        sndRetryDelay = c.getNetworkSendRetryDelay();
        timeSrvPortBase = c.getTimeServerPortBase();
        timeSrvPortRange = c.getTimeServerPortRange();
        utilityCacheKeepAliveTime = c.getUtilityCacheKeepAliveTime();
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

    /**
     * @return Active on start flag.
     */
    public boolean isActiveOnStart() {
        return activeOnStart;
    }

    /**
     * @return Class name of address resolver instance.
     */
    public String getAddressResolver() {
        return addrRslvr;
    }

    /**
     * @return Flag indicating whether cache sanity check is enabled.
     */
    public boolean isCacheSanityCheckEnabled() {
        return cacheSanityCheckEnabled;
    }

    /**
     * @return User's class loader.
     */
    public String getClassLoader() {
        return clsLdr;
    }

    /**
     * Gets consistent globally unique node ID which survives node restarts.
     *
     * @return Node consistent ID.a
     */
    public String getConsistentId() {
        return consistentId;
    }

    /**
     * @return Failure detection timeout in milliseconds.
     */
    public Long getFailureDetectionTimeout() {
        return failureDetectionTimeout;
    }

    /**
     * @return Ignite work directory.
     */
    public String getWorkDirectory() {
        return igniteWorkDir;
    }

    /**
     * @return Late affinity assignment flag.
     */
    public boolean isLateAffinityAssignment() {
        return lateAffAssignment;
    }

    /**
     * @return {@code True} if local jobs should be marshalled.
     */
    public boolean isMarshalLocalJobs() {
        return marshLocJobs;
    }

    /**
     * @return Job metrics update frequency in milliseconds.
     */
    public long getMetricsUpdateFrequency() {
        return metricsUpdateFreq;
    }

    /**
     * @return Failure detection timeout for client nodes in milliseconds.
     */
    public Long getClientFailureDetectionTimeout() {
        return clientFailureDetectionTimeout;
    }

    /**
     * @return Message send retries count.
     */
    public int getNetworkSendRetryCount() {
        return sndRetryCnt;
    }

    /**
     * @return Interval between message send retries.
     */
    public long getNetworkSendRetryDelay() {
        return sndRetryDelay;
    }

    /**
     * @return Base UPD port number for grid time server.
     */
    public int getTimeServerPortBase() {
        return timeSrvPortBase;
    }

    /**
     * @return Number of ports to try before server initialization fails.
     */
    public int getTimeServerPortRange() {
        return timeSrvPortRange;
    }

    /**
     * @return Thread pool keep alive time (in milliseconds) to be used in grid for utility cache messages.
     */
    public long getUtilityCacheKeepAliveTime() {
        return utilityCacheKeepAliveTime;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, igniteInstanceName);
        U.writeString(out, ggHome);
        U.writeString(out, locHost);
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
        out.writeBoolean(activeOnStart);
        U.writeString(out, addrRslvr);
        out.writeBoolean(cacheSanityCheckEnabled);
        U.writeString(out, clsLdr);
        U.writeString(out, consistentId);
        out.writeObject(failureDetectionTimeout);
        U.writeString(out, igniteWorkDir);
        out.writeBoolean(lateAffAssignment);
        out.writeBoolean(marshLocJobs);
        out.writeLong(metricsUpdateFreq);
        out.writeObject(clientFailureDetectionTimeout);
        out.writeInt(sndRetryCnt);
        out.writeLong(sndRetryDelay);
        out.writeInt(timeSrvPortBase);
        out.writeInt(timeSrvPortRange);
        out.writeLong(utilityCacheKeepAliveTime);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        igniteInstanceName = U.readString(in);
        ggHome = U.readString(in);
        locHost = U.readString(in);
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
        activeOnStart = in.readBoolean();
        addrRslvr = U.readString(in);
        cacheSanityCheckEnabled = in.readBoolean();
        clsLdr = U.readString(in);
        consistentId = U.readString(in);
        failureDetectionTimeout = (Long)in.readObject();
        igniteWorkDir = U.readString(in);
        lateAffAssignment = in.readBoolean();
        marshLocJobs = in.readBoolean();
        metricsUpdateFreq = in.readLong();
        clientFailureDetectionTimeout = (Long)in.readObject();
        sndRetryCnt = in.readInt();
        sndRetryDelay = in.readLong();
        timeSrvPortBase = in.readInt();
        timeSrvPortRange = in.readInt();
        utilityCacheKeepAliveTime = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorBasicConfiguration.class, this);
    }
}
