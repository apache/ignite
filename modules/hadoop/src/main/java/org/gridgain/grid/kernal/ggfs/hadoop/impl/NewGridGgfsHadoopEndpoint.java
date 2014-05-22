/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop.impl;

import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.net.*;

import static org.gridgain.grid.ggfs.GridGgfsConfiguration.*;

/**
 * GGFS endpoint abstraction.
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
     * Normalize URI path.
     *
     * @param uri URI.
     * @return Normalized URI.
     */
    public static URI normalize(URI uri) {
        if (!F.eq(GridGgfs.GGFS_SCHEME, uri.getScheme()))
            throw new IllegalArgumentException("Normalization can only be applied to GGFS URI: " + uri);

        NewGridGgfsHadoopEndpoint endpoint = new NewGridGgfsHadoopEndpoint(uri.getAuthority());

        try {
            return new URI(uri.getScheme(), endpoint.ggfsName + ":" + endpoint.gridName, endpoint.host, endpoint.port,
                uri.getPath(), uri.getQuery(), uri.getFragment());
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException("Failed to normalize URI: " + uri, e);
        }
    }

    /**
     * Constructor.
     *
     * @param connStr Connection string.
     */
    NewGridGgfsHadoopEndpoint(@Nullable String connStr) {
        if (connStr == null)
            connStr = "";

        String[] tokens = connStr.split("@", -1);

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
                String[] authTokens = authStr.split(":", -1);

                ggfsName = F.isEmpty(authTokens[0]) ? null : authTokens[0];

                if (authTokens.length == 1)
                    gridName = null;
                else if (authTokens.length == 2)
                    gridName = F.isEmpty(authTokens[1]) ? null : authTokens[1];
                else
                    throw new IllegalArgumentException("Invalid connection string format: " + connStr);
            }

            hostPort = hostPort(connStr, tokens[1]);
        }
        else
            throw new IllegalArgumentException("Invalid connection string format: " + connStr);

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
     */
    private GridBiTuple<String, Integer> hostPort(String connStr, String hostPortStr) throws IllegalArgumentException {
        String[] tokens = hostPortStr.split(":", -1);

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
                    throw new IllegalArgumentException("Invalid port number: " + connStr);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid port number: " + connStr);
            }
        }
        else
            throw new IllegalArgumentException("Invalid connection string format: " + connStr);

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
