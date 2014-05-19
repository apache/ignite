/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop.impl;

import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

import static org.gridgain.grid.ggfs.GridGgfsConfiguration.*;

/**
 *
 */
public class NewGridGgfsHadoopEndpoint {
    /** Localhost. */
    public static final String LOCALHOST = "127.0.0.1";

    /** GGFS name. */
    private final String ggfsName;

    /** Grid name. */
    private final String gridName;

    /** Host. */
    private final String host;

    /** Port. */
    private final int port;

    /**
     * Constructor.
     *
     * @param connStr Connection string.
     * @throws java.io.IOException If failed to parse connection string.
     */
    NewGridGgfsHadoopEndpoint(String connStr) throws IOException {
        String[] tokens = connStr.split("@");

        GridBiTuple<String, Integer> hostPort;

        if (tokens.length == 1) {
            ggfsName = null;
            gridName = null;

            hostPort = hostPort(connStr, connStr);
        }
        else if (tokens.length == 2) {
            String authStr = tokens[0];

            if (authStr.isEmpty()) {
                gridName = null;
                ggfsName = null;
            }
            else {
                String[] authTokens = authStr.split(":");

                ggfsName = F.isEmpty(authTokens[0]) ? null : authTokens[0];

                if (authTokens.length == 1)
                    gridName = null;
                else if (authTokens.length == 2)
                    gridName = F.isEmpty(authTokens[1]) ? null : authTokens[1];
                else
                    throw new IOException("Invalid connection string format: " + connStr);
            }

            hostPort = hostPort(connStr, tokens[1]);
        }
        else
            throw new IOException("Invalid connection string format: " + connStr);

        host = hostPort.get1();

        assert hostPort.get2() != null;

        port = hostPort.get2();
    }

    /**
     * Parse host and port.
     *
     * @param connStr Full connection string.
     * @param hostPortStr Host/port connection string part.
     * @return Tuple with host and port.
     * @throws IOException If failed.
     */
    private GridBiTuple<String, Integer> hostPort(String connStr, String hostPortStr) throws IOException {
        String[] tokens = hostPortStr.split(":");

        String host = tokens[0];

        if (F.isEmpty(host))
            host = LOCALHOST;

        int port;

        if (tokens.length == 1)
            port = DFLT_IPC_PORT;
        else if (tokens.length == 2) {
            String portStr = tokens[1];

            try {
                port = Integer.valueOf(portStr);

                if (port < 0 || port > 65535)
                    throw new IOException("Invalid port number: " + connStr);
            }
            catch (NumberFormatException e) {
                throw new IOException("Invalid port number: " + connStr);
            }
        }
        else
            throw new IOException("Invalid connection string format: " + connStr);

        return F.t(host, port);
    }

    /**
     * @return GGFS name.
     */
    @Nullable public String ggfs() {
        return ggfsName;
    }

    /**
     * @return Grid name.
     */
    @Nullable public String grid() {
        return gridName;
    }

    /**
     * @return Host.
     */
    public String host() {
        return host;
    }

    /**
     * @return Host.
     */
    public boolean isLocal() {
        return F.eq(LOCALHOST, host);
    }

    /**
     * @return Port.
     */
    public int port() {
        return port;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NewGridGgfsHadoopEndpoint.class, this);
    }
}
