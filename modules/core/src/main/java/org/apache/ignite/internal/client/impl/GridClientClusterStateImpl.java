/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientPredicate;
import org.apache.ignite.internal.client.balancer.GridClientLoadBalancer;
import org.apache.ignite.internal.client.impl.connection.GridClientConnection;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.IgniteFeatures.FORCED_CHANGE_OF_CLUSTER_STATE;
import static org.apache.ignite.internal.client.util.GridClientUtils.checkFeatureSupportedByCluster;

/**
 *
 */
public class GridClientClusterStateImpl extends GridClientAbstractProjection<GridClientClusterStateImpl>
    implements GridClientClusterState {
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
        state(active ? ACTIVE : INACTIVE);
    }

    /** {@inheritDoc} */
    @Override public boolean active() throws GridClientException {
        return withReconnectHandling(GridClientConnection::currentState).get();
    }

    /** {@inheritDoc} */
    @Override public ClusterState state() throws GridClientException {
        return withReconnectHandling(GridClientConnection::state).get();
    }

    /** {@inheritDoc} */
    @Override public void state(ClusterState newState) throws GridClientException {
        state(newState, true);
    }

    /** {@inheritDoc} */
    @Override public void state(ClusterState newState, boolean force) throws GridClientException {
        // Check compapability of new forced deactivation on all nodes.
        UUID oldVerServerNode = checkFeatureSupportedByCluster(client, FORCED_CHANGE_OF_CLUSTER_STATE,
            false, false);

        // Send new version of state change request supporting 'force' feature.
        if (oldVerServerNode == null)
            withReconnectHandling((con, nodeId) -> con.changeState(newState, force, nodeId)).get();
        else {
            if (!force) {
                throw new GridClientException("Unable to change state of cluster on \""
                    + newState.name() + "\". Found a node not supporting safe deactivation: "
                    + oldVerServerNode + ". It can cause no checking of deactivation safety "
                    + "will be performed. You can try with the flag 'force' Be aware that deactivation erases in-memory"
                    + " data.");
            }
            else {
                // Send old version of the command not supporting 'force' feature.
                withReconnectHandling((con, nodeId) -> con.changeState(newState, nodeId)).get();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String clusterName() throws GridClientException {
        return withReconnectHandling(GridClientConnection::clusterName).get();
    }
}
