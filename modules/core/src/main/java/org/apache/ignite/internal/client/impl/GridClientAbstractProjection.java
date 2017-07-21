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

package org.apache.ignite.internal.client.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.client.GridClientClosedException;
import org.apache.ignite.internal.client.GridClientDataAffinity;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFuture;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientPredicate;
import org.apache.ignite.internal.client.GridServerUnreachableException;
import org.apache.ignite.internal.client.balancer.GridClientLoadBalancer;
import org.apache.ignite.internal.client.impl.connection.GridClientConnection;
import org.apache.ignite.internal.client.impl.connection.GridClientConnectionResetException;
import org.apache.ignite.internal.client.impl.connection.GridConnectionIdleClosedException;
import org.apache.ignite.internal.client.util.GridClientUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.client.util.GridClientUtils.applyFilter;
import static org.apache.ignite.internal.client.util.GridClientUtils.restAvailable;

/**
 * Class contains common connection-error handling logic.
 */
abstract class GridClientAbstractProjection<T extends GridClientAbstractProjection> {
    /** Logger. */
    private static final Logger log = Logger.getLogger(GridClientAbstractProjection.class.getName());

    /** List of nodes included in this projection. If null, all nodes in topology are included. */
    protected Collection<GridClientNode> nodes;

    /** Node filter to be applied for this projection. */
    protected GridClientPredicate<? super GridClientNode> filter;

    /** Balancer to be used in this projection. */
    protected GridClientLoadBalancer balancer;

    /** Count of reconnect retries before exception is thrown. */
    private static final int RETRY_CNT = 3;

    /** Retry delay. */
    private static final int RETRY_DELAY = 1000;

    /** Client instance. */
    protected GridClientImpl client;

    /**
     * Creates projection with specified client.
     *
     * @param client Client instance to use.
     * @param nodes Collections of nodes included in this projection.
     * @param filter Node filter to be applied.
     * @param balancer Balancer to use.
     */
    protected GridClientAbstractProjection(GridClientImpl client, Collection<GridClientNode> nodes,
        GridClientPredicate<? super GridClientNode> filter, GridClientLoadBalancer balancer) {
        assert client != null;

        this.client = client;
        this.nodes = nodes;
        this.filter = filter;
        this.balancer = balancer;
    }

    /**
     * This method executes request to a communication layer and handles connection error, if it occurs.
     * In case of communication exception client instance is notified and new instance of client is created.
     * If none of the grid servers can be reached, an exception is thrown.
     *
     * @param c Closure to be executed.
     * @param <R> Result future type.
     * @return Future returned by closure.
     */
    protected <R> GridClientFuture<R> withReconnectHandling(ClientProjectionClosure<R> c) {
        try {
            GridClientNode node = null;

            boolean changeNode = false;

            Throwable cause = null;

            for (int i = 0; i < RETRY_CNT; i++) {
                if (node == null || changeNode)
                    try {
                        node = balancedNode(node);
                    }
                    catch (GridClientException e) {
                        if (node == null)
                            throw e;
                        else
                            throw new GridServerUnreachableException(
                                "All nodes in projection failed when retrying to perform request. Attempts made: " + i,
                                cause);
                    }

                GridClientConnection conn = null;

                try {
                    conn = client.connectionManager().connection(node);

                    return c.apply(conn, node.nodeId());
                }
                catch (GridConnectionIdleClosedException e) {
                    client.connectionManager().terminateConnection(conn, node, e);

                    // It's ok, just reconnect to the same node.
                    changeNode = false;

                    cause = e;
                }
                catch (GridClientConnectionResetException e) {
                    client.connectionManager().terminateConnection(conn, node, e);

                    changeNode = true;

                    cause = e;
                }
                catch (GridServerUnreachableException e) {
                    changeNode = true;

                    cause = e;
                }

                U.sleep(RETRY_DELAY);
            }

            assert cause != null;

            throw new GridServerUnreachableException("Failed to communicate with grid nodes " +
                "(maximum count of retries reached).", cause);
        }
        catch (GridClientException e) {
            return new GridClientFutureAdapter<>(e);
        }
        catch (IgniteInterruptedCheckedException | InterruptedException e) {
            Thread.currentThread().interrupt();

            return new GridClientFutureAdapter<>(
                new GridClientException("Interrupted when (re)trying to perform request.", e));
        }
    }

    /**
     * This method executes request to a communication layer and handles connection error, if it occurs. Server
     * is picked up according to the projection affinity and key given. Connection will be made with the node
     * on which key is cached. In case of communication exception client instance is notified and new instance
     * of client is created. If none of servers can be reached, an exception is thrown.
     *
     * @param c Closure to be executed.
     * @param cacheName Cache name for which mapped node will be calculated.
     * @param affKey Affinity key.
     * @param <R> Type of result in future.
     * @return Operation future.
     */
    protected <R> GridClientFuture<R> withReconnectHandling(ClientProjectionClosure<R> c, String cacheName,
        @Nullable Object affKey) {
        GridClientDataAffinity affinity = client.affinity(cacheName);

        // If pinned (fixed-nodes) or no affinity provided use balancer.
        if (nodes != null || affinity == null || affKey == null)
            return withReconnectHandling(c);

        try {
            Collection<? extends GridClientNode> prjNodes = projectionNodes();

            if (prjNodes.isEmpty())
                throw new GridServerUnreachableException("Failed to get affinity node (no nodes in topology were " +
                    "accepted by the filter): " + filter);

            GridClientNode node = affinity.node(affKey, prjNodes);

            for (int i = 0; i < RETRY_CNT; i++) {
                GridClientConnection conn = null;

                try {
                    conn = client.connectionManager().connection(node);

                    return c.apply(conn, node.nodeId());
                }
                catch (GridConnectionIdleClosedException e) {
                    client.connectionManager().terminateConnection(conn, node, e);
                }
                catch (GridClientConnectionResetException e) {
                    client.connectionManager().terminateConnection(conn, node, e);

                    if (!checkNodeAlive(node.nodeId()))
                        throw new GridServerUnreachableException("Failed to communicate with mapped grid node for " +
                            "given affinity key (node left the grid) [nodeId=" + node.nodeId() + ", affKey=" + affKey +
                            ']', e);
                }
                catch (RuntimeException | Error e) {
                    if (conn != null)
                        client.connectionManager().terminateConnection(conn, node, e);

                    throw e;
                }

                U.sleep(RETRY_DELAY);
            }

            throw new GridServerUnreachableException("Failed to communicate with mapped grid node for given affinity " +
                "key (did node leave the grid?) [nodeId=" + node.nodeId() + ", affKey=" + affKey + ']');
        }
        catch (GridClientException e) {
            return new GridClientFutureAdapter<>(e);
        }
        catch (IgniteInterruptedCheckedException | InterruptedException e) {
            Thread.currentThread().interrupt();

            return new GridClientFutureAdapter<>(new GridClientException("Interrupted when (re)trying to perform " +
                "request.", e));
        }
    }

    /**
     * Tries to refresh node on every possible connection in topology.
     *
     * @param nodeId Node id to check.
     * @return {@code True} if response was received, {@code false} if either {@code null} response received or
     *      no nodes can be contacted at all.
     * @throws GridClientException If failed to refresh topology or if client was closed manually.
     * @throws InterruptedException If interrupted.
     */
    protected boolean checkNodeAlive(UUID nodeId) throws GridClientException, InterruptedException {
        // Try to get node information on any of the connections possible.
        for (GridClientNodeImpl node : client.topology().nodes()) {
            try {
                // Do not try to connect to the same node.
                if (node.nodeId().equals(nodeId))
                    continue;

                GridClientConnection conn = client.connectionManager().connection(node);

                try {
                    GridClientNode target = conn.node(nodeId, false, false, node.nodeId()).get();

                    if (target == null)
                        client.topology().nodeFailed(nodeId);

                    return target != null;
                }
                catch (GridClientConnectionResetException e) {
                    client.connectionManager().terminateConnection(conn, node, e);
                }
                catch (GridClientClosedException e) {
                    throw e;
                }
                catch (GridClientException e) {
                    if (log.isLoggable(Level.FINE))
                        log.log(Level.FINE, "Node request failed, try next node.", e);
                }
            }
            catch (GridServerUnreachableException e) {
                if (log.isLoggable(Level.FINE))
                    log.log(Level.FINE, "Node request failed, try next node.", e);
            }
        }

        return false;
    }

    /**
     * Gets most recently refreshed topology. If this compute instance is a projection,
     * then only nodes that satisfy projection criteria will be returned.
     *
     * @return Most recently refreshed topology.
     * @throws GridClientException If failed to refresh topology.
     */
    public Collection<? extends GridClientNode> projectionNodes() throws GridClientException {
        return projectionNodes(null);
    }

    /**
     * Gets most recently refreshed topology. If this compute instance is a projection,
     * then only nodes that satisfy projection criteria will be returned.
     *
     * @param pred Predicate to additionally filter projection nodes,
     *  if {@code null} just return projection.
     * @return Most recently refreshed topology.
     * @throws GridClientException If failed to refresh topology.
     */
    protected Collection<? extends GridClientNode> projectionNodes(@Nullable GridClientPredicate<GridClientNode> pred)
        throws GridClientException {
        Collection<? extends GridClientNode> prjNodes;

        if (nodes == null) {
            // Dynamic projection, ask topology for current snapshot.
            prjNodes = client.topology().nodes();

            if (filter != null || pred != null)
                prjNodes = applyFilter(prjNodes, filter, pred);
        }
        else
            prjNodes = nodes;

        return prjNodes;
    }

    /**
     * Return balanced node for current projection.
     *
     * @param exclude Nodes to exclude.
     * @return Balanced node.
     * @throws GridServerUnreachableException If topology is empty.
     */
    private GridClientNode balancedNode(@Nullable final GridClientNode exclude) throws GridClientException {
        GridClientPredicate<GridClientNode> excludeFilter = exclude == null ?
            new GridClientPredicate<GridClientNode>() {
                @Override public boolean apply(GridClientNode e) {
                    return restAvailable(e, client.cfg.getProtocol());
                }

                @Override public String toString() {
                    return "Filter nodes with available REST.";
                }
            } :
            new GridClientPredicate<GridClientNode>() {
                @Override public boolean apply(GridClientNode e) {
                    return !exclude.equals(e) && restAvailable(e, client.cfg.getProtocol());
                }

                @Override public String toString() {
                    return "Filter nodes with available REST and " +
                        "exclude (probably due to connection failure) node: " + exclude.nodeId();
                }
            };

        Collection<? extends GridClientNode> prjNodes = projectionNodes(excludeFilter);

        if (prjNodes.isEmpty())
            throw new GridServerUnreachableException("Failed to get balanced node (no nodes in topology were " +
                "accepted by the filters): " + Arrays.asList(filter, excludeFilter));

        if (prjNodes.size() == 1) {
            GridClientNode ret = GridClientUtils.first(prjNodes);

            assert ret != null;

            return ret;
        }

        return balancer.balancedNode(prjNodes);
    }

    /**
     * Creates a sub-projection for current projection.
     *
     * @param nodes Collection of nodes that sub-projection will be restricted to. If {@code null},
     *      created projection is dynamic and will take nodes from topology.
     * @param filter Filter to be applied to nodes in projection. If {@code null} - no filter applied.
     * @param balancer Balancer to use in projection. If {@code null} - inherit balancer from the current projection.
     * @param factory Factory to create new projection.
     * @return Created projection.
     * @throws GridClientException If resulting projection is empty. Note that this exception may
     *      only be thrown on case of static projections, i.e. where collection of nodes is not null.
     */
    protected T createProjection(@Nullable Collection<GridClientNode> nodes,
        @Nullable GridClientPredicate<? super GridClientNode> filter, @Nullable GridClientLoadBalancer balancer,
        ProjectionFactory<T> factory) throws GridClientException {
        if (nodes != null && nodes.isEmpty())
            throw new GridClientException("Failed to create projection: given nodes collection is empty.");

        if (filter != null && this.filter != null)
            filter = new GridClientAndPredicate<>(this.filter, filter);
        else if (filter == null)
            filter = this.filter;

        Collection<GridClientNode> subset = intersection(this.nodes, nodes);

        if (subset != null && subset.isEmpty())
            throw new GridClientException("Failed to create projection (given node set does not overlap with " +
                "existing node set) [prjNodes=" + this.nodes + ", nodes=" + nodes);

        if (filter != null && subset != null) {
            subset = applyFilter(subset, filter);

            if (subset != null && subset.isEmpty())
                throw new GridClientException("Failed to create projection (none of the nodes in projection node " +
                    "set passed the filter) [prjNodes=" + subset + ", filter=" + filter + ']');
        }

        if (balancer == null)
            balancer = this.balancer;

        return factory.create(nodes, filter, balancer);
    }

    /**
     * Calculates intersection of two collections. Returned collection always a new collection.
     *
     * @param first First collection to intersect.
     * @param second Second collection to intersect.
     * @return Intersection or {@code null} if both collections are {@code null}.
     */
    @Nullable private Collection<GridClientNode> intersection(@Nullable Collection<? extends GridClientNode> first,
        @Nullable Collection<? extends GridClientNode> second) {
        if (first == null && second == null)
            return null;

        if (first != null && second != null) {
            Collection<GridClientNode> res = new LinkedList<>(first);

            res.retainAll(second);

            return res;
        }
        else
            return new ArrayList<>(first != null ? first : second);
    }

    /**
     * Factory for new projection creation.
     *
     * @param <X> Projection implementation.
     */
    protected static interface ProjectionFactory<X extends GridClientAbstractProjection> {
        /**
         * Subclasses must implement this method and return concrete implementation of projection needed.
         *
         * @param nodes Nodes that are included in projection.
         * @param filter Filter to be applied.
         * @param balancer Balancer to be used.
         * @return Created projection.
         */
        public X create(@Nullable Collection<GridClientNode> nodes,
            @Nullable GridClientPredicate<? super GridClientNode> filter, GridClientLoadBalancer balancer);
    }

    /**
     * Closure to execute reconnect-handling code.
     */
    protected static interface ClientProjectionClosure<R> {
        /**
         * All closures that implement this interface may safely call all methods of communication connection.
         * If any exceptions in connection occur, they will be automatically handled by projection.
         *
         * @param conn Communication connection that should be accessed.
         * @param affinityNodeId Affinity node ID.
         * @return Future - result of operation.
         * @throws GridClientConnectionResetException If connection was unexpectedly reset. Connection will be
         *      either re-established or different server will be accessed (if available).
         * @throws GridClientClosedException If client was manually closed by user.
         */
        public GridClientFuture<R> apply(GridClientConnection conn, UUID affinityNodeId)
            throws GridClientConnectionResetException, GridClientClosedException;
    }
}