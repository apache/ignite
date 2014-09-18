/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment.uri.scanners.ftp;

import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

/**
 * URI FTP deployment configuration.
 */
class GridUriDeploymentFtpConfiguration {
    /** */
    private static final String DFLT_DIR = "/";

    /** */
    private String host;

    /** */
    private int port = 21;

    /** */
    private String username;

    /** */
    @GridToStringExclude private String pswd;

    /** */
    private String dir = DFLT_DIR;

    /**
     * @return Host.
     */
    String getHost() { return host; }

    /**
     * @param host FTP host.
     */
    void setHost(String host) {
        assert host != null;

        this.host = host;
    }

    /**
     * @return Port.
     */
    int getPort() { return port; }

    /**
     * @param port FTP port.
     */
    void setPort(int port) {
        assert port > 0;

        this.port = port;
    }

    /**
     * @return Username.
     */
    String getUsername() { return username; }

    /**
     * @param username FTP username.
     */
    void setUsername(String username) {
        assert username != null;

        this.username = username;
    }

    /**
     * @return Password.
     */
    String getPassword() { return pswd; }

    /**
     * @param pswd FTP password.
     */
    void setPassword(String pswd) {
        assert pswd != null;

        this.pswd = pswd;
    }

    /**
     * @return Directory.
     */
    String getDirectory() { return dir; }

    /**
     * @param dir FTP remote directory.
     */
    void setDirectory(String dir) { this.dir = dir == null ? DFLT_DIR : dir; }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridUriDeploymentFtpConfiguration.class, this);
    }
}
