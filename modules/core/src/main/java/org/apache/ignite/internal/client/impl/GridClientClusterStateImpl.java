/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client.impl;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.client.GridClientClosedException;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFuture;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientPredicate;
import org.apache.ignite.internal.client.balancer.GridClientLoadBalancer;
import org.apache.ignite.internal.client.impl.connection.GridClientConnection;
import org.apache.ignite.internal.client.impl.connection.GridClientConnectionResetException;
import org.apache.ignite.internal.client.impl.id_and_tag.IdAndTagViewTask;
import org.apache.ignite.internal.client.impl.id_and_tag.IdAndTagViewTaskResult;
import org.apache.ignite.internal.visor.VisorTaskArgument;

/**
 *
 */
public class GridClientClusterStateImpl extends GridClientAbstractProjection<GridClientClusterStateImpl>
    implements GridClientClusterState {
    /**
     * Closure to execute Cluster ID and Tag view action on cluster.
     */
    private static final ClientProjectionClosure<IdAndTagViewTaskResult> ID_AND_TAG_VIEW_CL = (conn, nodeId) ->
        conn.execute(
            IdAndTagViewTask.class.getName(),
            new VisorTaskArgument<>(nodeId, null, false),
            nodeId,
            false
        );

    /**
     * Creates projection with specified client.
     *
     * @param client Client instance to use.
     * @param nodes Collections of nodes included in this projection.
     * @param filter Node filter to be applied.
     * @param balancer Balancer to use.
     */
    public GridClientClusterStateImpl(
        GridClientImpl client,
        Collection<GridClientNode> nodes,
        GridClientPredicate<? super GridClientNode> filter,
        GridClientLoadBalancer balancer
    ) {
        super(client, nodes, filter, balancer);
    }

    /** {@inheritDoc} */
    @Override public void active(final boolean active) throws GridClientException {
        withReconnectHandling(new ClientProjectionClosure<Void>() {
            @Override public GridClientFuture apply(
                GridClientConnection conn,
                UUID nodeId
            ) throws GridClientConnectionResetException, GridClientClosedException {
                return conn.changeState(active, nodeId);
            }
        }).get();
    }

    /** {@inheritDoc} */
    @Override public boolean active() throws GridClientException {
        return withReconnectHandling(GridClientConnection::currentState).get();
    }

    /** {@inheritDoc} */
    @Override public boolean readOnly() throws GridClientException {
        return withReconnectHandling(GridClientConnection::readOnlyState).get();
    }

    /** {@inheritDoc} */
    @Override public void readOnly(boolean readOnly) throws GridClientException {
        withReconnectHandling(new ClientProjectionClosure<Void>() {
            @Override public GridClientFuture apply(
                GridClientConnection conn,
                UUID nodeId
            ) throws GridClientConnectionResetException, GridClientClosedException {
                return conn.changeReadOnlyState(readOnly, nodeId);
            }
        }).get();
    }

    /** {@inheritDoc} */
    @Override public UUID id() throws GridClientException {
        return withReconnectHandling(ID_AND_TAG_VIEW_CL).get().id();
    }

    /** {@inheritDoc} */
    @Override public String tag() throws GridClientException {
        return withReconnectHandling(ID_AND_TAG_VIEW_CL).get().tag();
    }

    /** {@inheritDoc} */
    @Override public String clusterName() throws GridClientException {
        return withReconnectHandling(GridClientConnection::clusterName).get();
    }
}
