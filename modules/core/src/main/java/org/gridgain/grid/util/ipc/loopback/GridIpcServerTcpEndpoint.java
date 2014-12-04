/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.loopback;

import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.ipc.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Server loopback IPC endpoint.
 */
public class GridIpcServerTcpEndpoint implements GridIpcServerEndpoint {
    /** Default endpoint port number. */
    public static final int DFLT_IPC_PORT = 10500;

    /** Server socket. */
    private ServerSocket srvSock;

    /** Port to bind socket to. */
    private int port = DFLT_IPC_PORT;

    /** Host to bind socket to. */
    private String host;

    /** Management endpoint flag. */
    private boolean mgmt;

    /** Logger. */
    @IgniteLoggerResource
    private GridLogger log;

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        if (port <= 0 || port >= 0xffff)
            throw new GridIpcEndpointBindException("Port value is illegal: " + port);

        try {
            srvSock = new ServerSocket();

            assert host != null;

            srvSock.bind(new InetSocketAddress(U.resolveLocalHost(host), port));

            if (log.isInfoEnabled())
                log.info("IPC server loopback endpoint started [port=" + port + ']');
        }
        catch (IOException e) {
            if (srvSock != null)
                U.closeQuiet(srvSock);

            throw new GridIpcEndpointBindException("Failed to bind loopback IPC endpoint (is port already in " +
                "use?): " + port, e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridIpcEndpoint accept() throws GridException {
        try {
            Socket sock = srvSock.accept();

            return new GridIpcClientTcpEndpoint(sock);
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(srvSock);
    }

    /** {@inheritDoc} */
    @Override public int getPort() {
        return port;
    }

    /**
     * Sets port endpoint will be bound to.
     *
     * @param port Port number.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /** {@inheritDoc} */
    @Override public String getHost() {
        return host;
    }

    /**
     * Sets host endpoint will be bound to.
     *
     * @param host Host.
     */
    public void setHost(String host) {
        this.host = host;
    }

    /** {@inheritDoc} */
    @Override public boolean isManagement() {
        return mgmt;
    }

    /**
     * Sets management property.
     *
     * @param mgmt flag.
     */
    public void setManagement(boolean mgmt) {
        this.mgmt = mgmt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridIpcServerTcpEndpoint.class, this);
    }

    /**
     * Sets configuration properties from the map.
     *
     * @param endpointCfg Map of properties.
     * @throws GridException If invalid property name or value.
     */
    public void setupConfiguration(Map<String, String> endpointCfg) throws GridException {
        for (Map.Entry<String,String> e : endpointCfg.entrySet()) {
            try {
                switch (e.getKey()) {
                    case "type":
                        //Ignore this property
                        break;

                    case "port":
                        setPort(Integer.parseInt(e.getValue()));
                        break;

                    case "host":
                        setHost(e.getValue());
                        break;

                    case "management":
                        setManagement(Boolean.valueOf(e.getValue()));
                        break;

                    default:
                        throw new GridException("Invalid property '" + e.getKey() + "' of " + getClass().getSimpleName());
                }
            }
            catch (Throwable t) {
                if (t instanceof GridException)
                    throw t;

                throw new GridException("Invalid value '" + e.getValue() + "' of the property '" + e.getKey() + "' in " +
                    getClass().getSimpleName(), t);
            }
        }
    }
}
