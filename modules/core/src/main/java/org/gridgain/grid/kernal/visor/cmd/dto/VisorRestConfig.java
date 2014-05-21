/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Node REST configuration.
 */
public class VisorRestConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether REST enabled or not. */
    private final boolean restEnabled;

    /** Whether or not SSL is enabled for TCP binary protocol. */
    private final boolean tcpSslEnabled;

    /** Rest accessible folders (log command can get files from). */
    private final String[] accessibleFolders;

    /** Jetty config path. */
    private final String jettyPath;

    /** Jetty host. */
    private final String jettyHost;

    /** Jetty port. */
    private final Integer jettyPort;

    /** REST TCP binary host. */
    private final String tcpHost;

    /** REST TCP binary port. */
    private final Integer tcpPort;

    /** Context factory for SSL. */
    private final String tcpSslContextFactory;

    public VisorRestConfig(boolean restEnabled, boolean tcpSslEnabled, String[] accessibleFolders,
        String jettyPath, String jettyHost, Integer jettyPort, String tcpHost, Integer tcpPort, String tcpSslContextFactory) {
        this.restEnabled = restEnabled;
        this.tcpSslEnabled = tcpSslEnabled;
        this.accessibleFolders = accessibleFolders;
        this.jettyPath = jettyPath;
        this.jettyHost = jettyHost;
        this.jettyPort = jettyPort;
        this.tcpHost = tcpHost;
        this.tcpPort = tcpPort;
        this.tcpSslContextFactory = tcpSslContextFactory;
    }

    /**
     * @return Whether REST enabled or not.
     */
    public boolean restEnabled() {
        return restEnabled;
    }

    /**
     * @return Whether or not SSL is enabled for TCP binary protocol.
     */
    public boolean tcpSslEnabled() {
        return tcpSslEnabled;
    }

    /**
     * @return Rest accessible folders (log command can get files from).
     */
    @Nullable public String[] accessibleFolders() {
        return accessibleFolders;
    }

    /**
     * @return Jetty config path.
     */
    @Nullable public String jettyPath() {
        return jettyPath;
    }

    /**
     * @return Jetty host.
     */
    @Nullable public String jettyHost() {
        return jettyHost;
    }

    /**
     * @return Jetty port.
     */
    @Nullable public Integer jettyPort() {
        return jettyPort;
    }

    /**
     * @return REST TCP binary host.
     */
    @Nullable public String tcpHost() {
        return tcpHost;
    }

    /**
     * @return REST TCP binary port.
     */
    @Nullable public Integer tcpPort() {
        return tcpPort;
    }

    /**
     * @return Context factory for SSL.
     */
    @Nullable public String tcpSslContextFactory() {
        return tcpSslContextFactory;
    }
}
