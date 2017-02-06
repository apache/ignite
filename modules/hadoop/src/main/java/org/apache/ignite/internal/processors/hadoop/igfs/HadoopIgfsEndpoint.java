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

package org.apache.ignite.internal.processors.hadoop.igfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * IGFS endpoint abstraction.
 */
public class HadoopIgfsEndpoint {
    /** Localhost. */
    public static final String LOCALHOST = "127.0.0.1";

    /** IGFS name. */
    private final String igfsName;

    /** Grid name. */
    private final String gridName;

    /** Host. */
    private final String host;

    /** Port. */
    private final int port;

    /**
     * Normalize IGFS URI.
     *
     * @param uri URI.
     * @return Normalized URI.
     * @throws IOException If failed.
     */
    public static URI normalize(URI uri) throws IOException {
        try {
            if (!F.eq(IgniteFileSystem.IGFS_SCHEME, uri.getScheme()))
                throw new IOException("Failed to normalize UIR because it has non IGFS scheme: " + uri);

            HadoopIgfsEndpoint endpoint = new HadoopIgfsEndpoint(uri.getAuthority());

            StringBuilder sb = new StringBuilder();

            if (endpoint.igfs() != null)
                sb.append(endpoint.igfs());

            if (endpoint.grid() != null)
                sb.append(":").append(endpoint.grid());

            return new URI(uri.getScheme(), sb.length() != 0 ? sb.toString() : null, endpoint.host(), endpoint.port(),
                uri.getPath(), uri.getQuery(), uri.getFragment());
        }
        catch (URISyntaxException | IgniteCheckedException e) {
            throw new IOException("Failed to normalize URI: " + uri, e);
        }
    }

    /**
     * Constructor.
     *
     * @param connStr Connection string.
     * @throws IgniteCheckedException If failed to parse connection string.
     */
    public HadoopIgfsEndpoint(@Nullable String connStr) throws IgniteCheckedException {
        if (connStr == null)
            connStr = "";

        String[] tokens = connStr.split("@", -1);

        IgniteBiTuple<String, Integer> hostPort;

        if (tokens.length == 1) {
            igfsName = null;
            gridName = null;

            hostPort = hostPort(connStr, connStr);
        }
        else if (tokens.length == 2) {
            String authStr = tokens[0];

            if (authStr.isEmpty()) {
                gridName = null;
                igfsName = null;
            }
            else {
                String[] authTokens = authStr.split(":", -1);

                igfsName = F.isEmpty(authTokens[0]) ? null : authTokens[0];

                if (authTokens.length == 1)
                    gridName = null;
                else if (authTokens.length == 2)
                    gridName = F.isEmpty(authTokens[1]) ? null : authTokens[1];
                else
                    throw new IgniteCheckedException("Invalid connection string format: " + connStr);
            }

            hostPort = hostPort(connStr, tokens[1]);
        }
        else
            throw new IgniteCheckedException("Invalid connection string format: " + connStr);

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
     * @throws IgniteCheckedException If failed to parse connection string.
     */
    private IgniteBiTuple<String, Integer> hostPort(String connStr, String hostPortStr) throws IgniteCheckedException {
        String[] tokens = hostPortStr.split(":", -1);

        String host = tokens[0];

        if (F.isEmpty(host))
            host = LOCALHOST;

        int port;

        if (tokens.length == 1)
            port = IgfsIpcEndpointConfiguration.DFLT_PORT;
        else if (tokens.length == 2) {
            String portStr = tokens[1];

            try {
                port = Integer.valueOf(portStr);

                if (port < 0 || port > 65535)
                    throw new IgniteCheckedException("Invalid port number: " + connStr);
            }
            catch (NumberFormatException ignored) {
                throw new IgniteCheckedException("Invalid port number: " + connStr);
            }
        }
        else
            throw new IgniteCheckedException("Invalid connection string format: " + connStr);

        return F.t(host, port);
    }

    /**
     * @return IGFS name.
     */
    @Nullable public String igfs() {
        return igfsName;
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
        return S.toString(HadoopIgfsEndpoint.class, this);
    }
}
