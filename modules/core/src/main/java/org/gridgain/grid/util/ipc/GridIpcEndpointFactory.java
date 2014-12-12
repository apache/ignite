/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.ipc.loopback.*;
import org.gridgain.grid.util.ipc.shmem.*;

/**
 * Ggfs endpoint factory for inter-process communication.
 */
public class GridIpcEndpointFactory {
    /**
     * Connects to open server IPC endpoint.
     *
     * @param endpointAddr Endpoint address.
     * @param log Log.
     * @return Connected client endpoint.
     * @throws IgniteCheckedException If failed to establish connection.
     */
    public static GridIpcEndpoint connectEndpoint(String endpointAddr, IgniteLogger log) throws IgniteCheckedException {
        A.notNull(endpointAddr, "endpointAddr");

        String[] split = endpointAddr.split(":");

        int port;

        if (split.length == 2) {
            try {
                port = Integer.parseInt(split[1]);
            }
            catch (NumberFormatException e) {
                throw new IgniteCheckedException("Failed to parse port number: " + endpointAddr, e);
            }
        }
        else
            // Use default port.
            port = -1;

        return "shmem".equalsIgnoreCase(split[0]) ?
            connectSharedMemoryEndpoint(port > 0 ? port : GridIpcSharedMemoryServerEndpoint.DFLT_IPC_PORT, log) :
            connectTcpEndpoint(split[0], port > 0 ? port : GridIpcServerTcpEndpoint.DFLT_IPC_PORT);
    }

    /**
     * Connects loopback IPC endpoint.
     *
     * @param host Loopback host.
     * @param port Loopback endpoint port.
     * @return Connected client endpoint.
     * @throws IgniteCheckedException If connection failed.
     */
    private static GridIpcEndpoint connectTcpEndpoint(String host, int port) throws IgniteCheckedException {
       return new GridIpcClientTcpEndpoint(host, port);
    }

    /**
     * Connects IPC shared memory endpoint.
     *
     * @param port Endpoint port.
     * @param log Log.
     * @return Connected client endpoint.
     * @throws IgniteCheckedException If connection failed.
     */
    private static GridIpcEndpoint connectSharedMemoryEndpoint(int port, IgniteLogger log) throws IgniteCheckedException {
        return new GridIpcSharedMemoryClientEndpoint(port, log);
    }
}
