/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.node;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

import static java.lang.System.*;
import static org.gridgain.grid.IgniteSystemProperties.*;
import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Create data transfer object for node REST configuration properties.
 */
public class VisorRestConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether REST enabled or not. */
    private boolean restEnabled;

    /** Whether or not SSL is enabled for TCP binary protocol. */
    private boolean tcpSslEnabled;

    /** Rest accessible folders (log command can get files from). */
    private String[] accessibleFolders;

    /** Jetty config path. */
    private String jettyPath;

    /** Jetty host. */
    private String jettyHost;

    /** Jetty port. */
    private Integer jettyPort;

    /** REST TCP binary host. */
    private String tcpHost;

    /** REST TCP binary port. */
    private Integer tcpPort;

    /** Context factory for SSL. */
    private String tcpSslContextFactory;

    /**
     * @param c Grid configuration.
     * @return Create data transfer object for node REST configuration properties.
     */
    public static VisorRestConfiguration from(IgniteConfiguration c) {
        VisorRestConfiguration cfg = new VisorRestConfiguration();

        GridClientConnectionConfiguration clnCfg = c.getClientConnectionConfiguration();

        boolean restEnabled = clnCfg != null;

        cfg.restEnabled(restEnabled);

        if (restEnabled) {
            cfg.tcpSslEnabled(clnCfg.isRestTcpSslEnabled());
            cfg.accessibleFolders(clnCfg.getRestAccessibleFolders());
            cfg.jettyPath(clnCfg.getRestJettyPath());
            cfg.jettyHost(getProperty(GG_JETTY_HOST));
            cfg.jettyPort(intValue(GG_JETTY_PORT, null));
            cfg.tcpHost(clnCfg.getRestTcpHost());
            cfg.tcpPort(clnCfg.getRestTcpPort());
            cfg.tcpSslContextFactory(compactClass(clnCfg.getRestTcpSslContextFactory()));
        }

        return cfg;
    }

    /**
     * @return Whether REST enabled or not.
     */
    public boolean restEnabled() {
        return restEnabled;
    }

    /**
     * @param restEnabled New whether REST enabled or not.
     */
    public void restEnabled(boolean restEnabled) {
        this.restEnabled = restEnabled;
    }

    /**
     * @return Whether or not SSL is enabled for TCP binary protocol.
     */
    public boolean tcpSslEnabled() {
        return tcpSslEnabled;
    }

    /**
     * @param tcpSslEnabled New whether or not SSL is enabled for TCP binary protocol.
     */
    public void tcpSslEnabled(boolean tcpSslEnabled) {
        this.tcpSslEnabled = tcpSslEnabled;
    }

    /**
     * @return Rest accessible folders (log command can get files from).
     */
    @Nullable public String[] accessibleFolders() {
        return accessibleFolders;
    }

    /**
     * @param accessibleFolders New rest accessible folders (log command can get files from).
     */
    public void accessibleFolders(String[] accessibleFolders) {
        this.accessibleFolders = accessibleFolders;
    }

    /**
     * @return Jetty config path.
     */
    @Nullable public String jettyPath() {
        return jettyPath;
    }

    /**
     * @param jettyPath New jetty config path.
     */
    public void jettyPath(String jettyPath) {
        this.jettyPath = jettyPath;
    }

    /**
     * @return Jetty host.
     */
    @Nullable public String jettyHost() {
        return jettyHost;
    }

    /**
     * @param jettyHost New jetty host.
     */
    public void jettyHost(String jettyHost) {
        this.jettyHost = jettyHost;
    }

    /**
     * @return Jetty port.
     */
    @Nullable public Integer jettyPort() {
        return jettyPort;
    }

    /**
     * @param jettyPort New jetty port.
     */
    public void jettyPort(Integer jettyPort) {
        this.jettyPort = jettyPort;
    }

    /**
     * @return REST TCP binary host.
     */
    @Nullable public String tcpHost() {
        return tcpHost;
    }

    /**
     * @param tcpHost New rEST TCP binary host.
     */
    public void tcpHost(String tcpHost) {
        this.tcpHost = tcpHost;
    }

    /**
     * @return REST TCP binary port.
     */
    @Nullable public Integer tcpPort() {
        return tcpPort;
    }

    /**
     * @param tcpPort New rEST TCP binary port.
     */
    public void tcpPort(Integer tcpPort) {
        this.tcpPort = tcpPort;
    }

    /**
     * @return Context factory for SSL.
     */
    @Nullable public String tcpSslContextFactory() {
        return tcpSslContextFactory;
    }

    /**
     * @param tcpSslCtxFactory New context factory for SSL.
     */
    public void tcpSslContextFactory(String tcpSslCtxFactory) {
        tcpSslContextFactory = tcpSslCtxFactory;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorRestConfiguration.class, this);
    }
}
