/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static java.lang.System.*;
import static org.gridgain.grid.GridSystemProperties.*;
import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Data transfer object for node basic configuration properties.
 */
public class VisorBasicConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Grid name. */
    private String gridName;

    /** GRIDGAIN_HOME determined at startup. */
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
    public static VisorBasicConfig from(GridEx g, GridConfiguration c) {
        VisorBasicConfig cfg = new VisorBasicConfig();

        cfg.gridName(c.getGridName());
        cfg.ggHome(getProperty(GG_HOME, c.getGridGainHome()));
        cfg.localHost(getProperty(GG_LOCAL_HOST, c.getLocalHost()));
        cfg.nodeId(g.localNode().id());
        cfg.marshaller(compactClass(c.getMarshaller()));
        cfg.deploymentMode(compactObject(c.getDeploymentMode()));
        cfg.daemon(boolValue(GG_DAEMON, c.isDaemon()));
        cfg.jmxRemote(g.isJmxRemoteEnabled());
        cfg.restart(g.isRestartEnabled());
        cfg.networkTimeout(c.getNetworkTimeout());
        cfg.licenseUrl(c.getLicenseUrl());
        cfg.logger(compactClass(c.getGridLogger()));
        cfg.discoStartupDelay(c.getDiscoveryStartupDelay());
        cfg.mBeanServer(compactClass(c.getMBeanServer()));
        cfg.noAscii(boolValue(GG_NO_ASCII, false));
        cfg.noDiscoOrder(boolValue(GG_NO_DISCO_ORDER, false));
        cfg.noShutdownHook(boolValue(GG_NO_SHUTDOWN_HOOK, false));
        cfg.programName(getProperty(GG_PROG_NAME));
        cfg.quiet(boolValue(GG_QUIET, true));
        cfg.successFile(getProperty(GG_SUCCESS_FILE));
        cfg.updateNotifier(boolValue(GG_UPDATE_NOTIFIER, true));
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
     * @return GRIDGAIN_HOME determined at startup.
     */
    @Nullable public String ggHome() {
        return ggHome;
    }

    /**
     * @param ggHome New GRIDGAIN_HOME determined at startup.
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
        return S.toString(VisorBasicConfig.class, this);
    }
}
