package org.gridgain.client.impl.connection;

import org.gridgain.client.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.util.*;

/**
 * Common interface for client connection managers.
 */
public interface GridClientConnectionManager {
    /**
     * Tries to open initial connection and fetch topology using given server addresses.
     *
     * @param srvs Collection<InetSocketAddress> server addresses.
     * @throws GridClientAuthenticationException If connection failed to authenticate on server.
     * @throws GridClientException If manager failed to initialise,
     * @throws InterruptedException If manager was interrupted while waiting for connection.
     */
    public void init(Collection<InetSocketAddress> srvs) throws GridClientException, InterruptedException;

    /**
     * Returns connection to the given node.
     *
     * @param node Node to connect with.
     * @return Connection to use for operations, targeted for the given node.
     * @throws GridServerUnreachableException If connection can't be established.
     * @throws InterruptedException If manager was interrupted while waiting for connection
     * to be established.
     * @throws GridClientClosedException If connection manager has been closed.
     */
    public GridClientConnection connection(GridClientNode node)
        throws GridServerUnreachableException, GridClientClosedException, InterruptedException;

    /**
     * Callback method, which should be called by clients when they get connectivity errors.
     * It's main purpose is to allow connection manager to terminate broken connection
     * early and, try to establish a new one for the consequent
     * {@link #connection(GridClientNode)} calls.
     *
     * @param conn Failed connection.
     * @param node Connected node.
     * @param e Error that caused connection termination.
     */
    public void terminateConnection(GridClientConnection conn, @Nullable GridClientNode node, Throwable e);

    /**
     * Stops this instance of connection manager and terminates all connections.
     * @param waitCompletion If {@code true} this method awaits termination of all connections
     *      (and receiving responses for all pending requests), otherwise it will return immediately.
     */
    public void stop(boolean waitCompletion);
}
