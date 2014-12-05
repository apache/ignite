/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;

import static org.apache.ignite.fs.IgniteFsConfiguration.*;

/**
 * GGFS endpoint abstraction.
 */
public class GridGgfsHadoopEndpoint {
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
     * Normalize GGFS URI.
     *
     * @param uri URI.
     * @return Normalized URI.
     * @throws IOException If failed.
     */
    public static URI normalize(URI uri) throws IOException {
        try {
            if (!F.eq(IgniteFs.GGFS_SCHEME, uri.getScheme()))
                throw new IOException("Failed to normalize UIR because it has non GGFS scheme: " + uri);

            GridGgfsHadoopEndpoint endpoint = new GridGgfsHadoopEndpoint(uri.getAuthority());

            StringBuilder sb = new StringBuilder();

            if (endpoint.ggfs() != null)
                sb.append(endpoint.ggfs());

            if (endpoint.grid() != null)
                sb.append(":").append(endpoint.grid());

            return new URI(uri.getScheme(), sb.length() != 0 ? sb.toString() : null, endpoint.host(), endpoint.port(),
                uri.getPath(), uri.getQuery(), uri.getFragment());
        }
        catch (URISyntaxException | GridException e) {
            throw new IOException("Failed to normalize URI: " + uri, e);
        }
    }

    /**
     * Constructor.
     *
     * @param connStr Connection string.
     * @throws GridException If failed to parse connection string.
     */
    public GridGgfsHadoopEndpoint(@Nullable String connStr) throws GridException {
        if (connStr == null)
            connStr = "";

        String[] tokens = connStr.split("@", -1);

        IgniteBiTuple<String, Integer> hostPort;

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
                    throw new GridException("Invalid connection string format: " + connStr);
            }

            hostPort = hostPort(connStr, tokens[1]);
        }
        else
            throw new GridException("Invalid connection string format: " + connStr);

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
     * @throws GridException If failed to parse connection string.
     */
    private IgniteBiTuple<String, Integer> hostPort(String connStr, String hostPortStr) throws GridException {
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
                    throw new GridException("Invalid port number: " + connStr);
            }
            catch (NumberFormatException e) {
                throw new GridException("Invalid port number: " + connStr);
            }
        }
        else
            throw new GridException("Invalid connection string format: " + connStr);

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
        return S.toString(GridGgfsHadoopEndpoint.class, this);
    }
}
