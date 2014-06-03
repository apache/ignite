/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Data transfer object for node basic configuration properties.
 */
public class VisorBasicConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Grid name. */
    private final String gridName;

    /** GRIDGAIN_HOME determined at startup. */
    private final String ggHome;

    /** Local host value used. */
    private final String locHost;

    /** Node id. */
    private final UUID nodeId;

    /** Marshaller used. */
    private final String marsh;

    /** Deploy Mode. */
    private final Object deployMode;

    /** Whether this node daemon or not. */
    private final boolean daemon;

    /** Whether remote JMX is enabled. */
    private final boolean jmxRemote;

    /** Is node restart enabled. */
    private final boolean restart;

    /** Network timeout. */
    private final long netTimeout;

    /** Node license URL */
    private final String licenseUrl;

    /** Logger used on node. */
    private final String log;

    /** Discovery startup delay. */
    private final long discoStartupDelay;

    /** MBean server name */
    private final String mBeanSrv;

    /** Whether ASCII logo is disabled. */
    private final boolean noAscii;

    /** Whether no discovery order is allowed. */
    private final boolean noDiscoOrder;

    /** Whether shutdown hook is disabled. */
    private final boolean noShutdownHook;

    /** Name of command line program. */
    private final String progName;

    /** Whether node is in quiet mode. */
    private final boolean quiet;

    /** Success file name. */
    private final String successFile;

    /** Whether update checker is enabled. */
    private final boolean updateNtf;

    /** Create data transfer object with given parameters. */
    public VisorBasicConfig(
        String gridName,
        String ggHome,
        String locHost,
        UUID nodeId,
        String marsh,
        Object deployMode,
        boolean daemon,
        boolean jmxRemote,
        boolean restart,
        long netTimeout,
        @Nullable String licenseUrl,
        String log,
        long discoStartupDelay,
        String mBeanSrv,
        boolean noAscii,
        boolean noDiscoOrder,
        boolean noShutdownHook,
        String progName,
        boolean quiet,
        String successFile,
        boolean updateNtf) {
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
     * @return GRIDGAIN_HOME determined at startup.
     */
    public String ggHome() {
        return ggHome;
    }

    /**
     * @return Local host value used.
     */
    public String localHost() {
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
     * @return Deploy Mode.
     */
    public Object deployMode() {
        return deployMode;
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
     * @return Node license URL
     */
    @Nullable public String licenseUrl() {
        return licenseUrl;
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
    public String mBeanServer() {
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
    public String program() {
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
}
