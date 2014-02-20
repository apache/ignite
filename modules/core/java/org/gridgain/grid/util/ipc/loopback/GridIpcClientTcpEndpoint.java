// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.loopback;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.ipc.*;

import java.io.*;
import java.net.*;

/**
 * Loopback IPC endpoint based on socket.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridIpcClientTcpEndpoint implements GridIpcEndpoint {
    /** Client socket. */
    private Socket clientSock;

    /**
     * Creates connected client IPC endpoint.
     *
     * @param clientSock Connected client socket.
     */
    public GridIpcClientTcpEndpoint(Socket clientSock) {
        assert clientSock != null;

        this.clientSock = clientSock;
    }

    /**
     * Creates and connects client IPC endpoint.
     *
     * @param port Port.
     * @param host Host.
     * @throws GridException If connection fails.
     */
    public GridIpcClientTcpEndpoint(String host, int port) throws GridException {
        clientSock = new Socket();

        try {
            clientSock.connect(new InetSocketAddress(host, port));
        }
        catch (IOException e) {
            throw new GridException("Failed to connect to endpoint [host=" + host + ", port=" + port + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public InputStream inputStream() throws GridException {
        try {
            return clientSock.getInputStream();
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public OutputStream outputStream() throws GridException {
        try {
            return clientSock.getOutputStream();
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(clientSock);
    }
}
