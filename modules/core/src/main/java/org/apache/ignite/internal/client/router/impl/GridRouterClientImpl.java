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

package org.apache.ignite.internal.client.router.impl;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientClosedException;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientData;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientPredicate;
import org.apache.ignite.internal.client.GridClientProtocol;
import org.apache.ignite.internal.client.GridClientTopologyListener;
import org.apache.ignite.internal.client.GridServerUnreachableException;
import org.apache.ignite.internal.client.impl.GridClientFutureAdapter;
import org.apache.ignite.internal.client.impl.GridClientImpl;
import org.apache.ignite.internal.client.impl.GridClientNodeImpl;
import org.apache.ignite.internal.client.impl.connection.GridClientConnection;
import org.apache.ignite.internal.client.impl.connection.GridClientConnectionManager;
import org.apache.ignite.internal.client.impl.connection.GridClientConnectionResetException;
import org.apache.ignite.internal.client.impl.connection.GridClientTopology;
import org.apache.ignite.internal.client.router.GridTcpRouterConfiguration;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.client.util.GridClientUtils.applyFilter;
import static org.apache.ignite.internal.client.util.GridClientUtils.restAvailable;

/**
 * A {@link GridClient} router implementation.
 */
public class GridRouterClientImpl implements GridClient {
    /** Decorated client implementation. */
    private final GridClientImpl clientImpl;

    /** Client configuration. */
    private final GridClientConfiguration cliCfg;

    /** TCP connection managers. */
    private final ConcurrentMap<Byte, GridClientConnectionManager> connMgrMap = new ConcurrentHashMap8<>();

    /**
     * Creates a new TCP client based on the given configuration.
     *
     * @param id Client identifier.
     * @param routerCfg Router configuration.
     * @throws GridClientException If client configuration is incorrect.
     * @throws GridServerUnreachableException If none of the servers
     *     specified in configuration can be reached.
     */
    GridRouterClientImpl(UUID id, GridTcpRouterConfiguration routerCfg) throws GridClientException {
        GridClientConfiguration cliCfg = new GridClientConfiguration();

        cliCfg.setServers(routerCfg.getServers());
        cliCfg.setSslContextFactory(routerCfg.getSslContextFactory());
        cliCfg.setSecurityCredentialsProvider(routerCfg.getSecurityCredentialsProvider());

        this.cliCfg = cliCfg;

        clientImpl = new GridClientImpl(id, cliCfg, true);

        if (cliCfg.getProtocol() != GridClientProtocol.TCP)
            throw new AssertionError("Unknown protocol: " + cliCfg.getProtocol());
    }

    /**
     * Send a raw packet "as is" directly to the given node.
     * The exact types of acceptable arguments and return values depends on underlying connections.
     *
     * @param msg Raw message to send.
     * @param destId Id of node to send message to. If {@code null} than node will be chosen
     *     from the topology randomly.
     * @return Future, representing forwarded message.
     * @throws GridServerUnreachableException If destination node can't be reached.
     * @throws GridClientClosedException If client is closed.
     * @throws GridClientException If any other client-related error occurs.
     * @throws InterruptedException If router was interrupted while trying.
     *     to establish connection with destination node.
     */
    GridClientFutureAdapter<?> forwardMessage(Object msg, @Nullable UUID destId, byte marshId)
        throws GridClientException, InterruptedException {
        GridClientTopology top = clientImpl.topology();

        GridClientNode dest = destId != null ?
            top.node(destId) : cliCfg.getBalancer().balancedNode(
                applyFilter(top.nodes(), new GridClientPredicate<GridClientNodeImpl>() {
                    @Override public boolean apply(GridClientNodeImpl e) {
                        return restAvailable(e, cliCfg.getProtocol());
                    }
                }));

        if (dest == null)
            throw new GridServerUnreachableException("Failed to resolve node for specified destination ID: " + destId);

        GridClientConnectionManager connMgr = connectionManager(marshId);

        GridClientConnection conn = null;

        // No reconnection handling there. Let client to do it if needed.
        GridClientException cause;

        try {
            conn = connMgr.connection(dest);

            return conn.forwardMessage(msg);
        }
        catch (GridClientConnectionResetException e) {
            if (destId != null)
                connMgr.terminateConnection(conn, top.node(destId), e);
            else
                connMgr.terminateConnection(conn, null, e);

            cause = e;
        }
        catch (GridClientException e) {
            cause = e;
        }

        GridClientFutureAdapter<Object> fail = new GridClientFutureAdapter<>();

        fail.onDone(cause);

        return fail;
    }

    /**
     * @param marshId Marshaller ID.
     * @return Connection manager.
     * @throws GridClientException In case of error.
     */
    private GridClientConnectionManager connectionManager(byte marshId) throws GridClientException {
        GridClientConnectionManager mgr = connMgrMap.get(marshId);

        if (mgr == null) {
            GridClientConnectionManager old = connMgrMap.putIfAbsent(marshId, mgr =
                clientImpl.newConnectionManager(marshId, true));

            if (old != null)
                mgr = old;
        }

        return mgr;
    }

    /**
     * Closes client.
     * @param wait If {@code true} will wait for all pending requests to be proceeded.
     */
    public void stop(boolean wait) {
        clientImpl.stop(wait);
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return clientImpl.id();
    }

    /** {@inheritDoc} */
    @Override public GridClientData data() throws GridClientException {
        return clientImpl.data();
    }

    /** {@inheritDoc} */
    @Override public GridClientData data(String cacheName) throws GridClientException {
        return clientImpl.data(cacheName);
    }

    /** {@inheritDoc} */
    @Override public GridClientCompute compute() {
        return clientImpl.compute();
    }

    /** {@inheritDoc} */
    @Override public void addTopologyListener(GridClientTopologyListener lsnr) {
        clientImpl.addTopologyListener(lsnr);
    }

    /** {@inheritDoc} */
    @Override public void removeTopologyListener(GridClientTopologyListener lsnr) {
        clientImpl.removeTopologyListener(lsnr);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridClientTopologyListener> topologyListeners() {
        return clientImpl.topologyListeners();
    }

    /** {@inheritDoc} */
    @Override public boolean connected() {
        return clientImpl.connected();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        clientImpl.close();
    }
}