/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.loopback;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.ipc.*;

import java.io.*;
import java.net.*;

/**
 * Server loopback IPC endpoint.
 *
 * @author @java.author
 * @version @java.version
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
    @GridLoggerResource
    private GridLogger log;

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        if (port <= 0 || port >= 0xffff)
            throw new GridGgfsIpcEndpointBindException("Port value is illegal: " + port);

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

            throw new GridGgfsIpcEndpointBindException("Failed to bind loopback IPC endpoint (is port already in " +
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
}
