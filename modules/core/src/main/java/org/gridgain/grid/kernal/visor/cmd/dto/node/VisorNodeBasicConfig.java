/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import java.io.*;
import java.util.*;

/**
 * Basic configuration data.
 */
public class VisorNodeBasicConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final String gridName;
    private final String ggHome;
    private final String locHost;
    private final UUID nodeId;
    private final String marsh;
    private final Object deployMode;
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

    public VisorNodeBasicConfig(String gridName, String ggHome, String locHost, UUID nodeId, String marsh,
        Object deployMode, boolean daemon, boolean jmxRemote, boolean restart, long netTimeout, String licenseUrl,
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
     * @return Marshaller.
     */
    public String marshaller() {
        return marsh;
    }

    /**
     * @return Deploy mode.
     */
    public Object deployMode() {
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
     * @return Mbean server.
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
     * @return Program name.
     */
    public String program() {
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
