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

package org.apache.ignite.internal.client;

import org.apache.ignite.cluster.ClusterState;

/**
 *  Interface for manage state of grid cluster.
 */
public interface GridClientClusterState {
    /**
     * @return Current cluster state.
     * @throws GridClientException If the request to get the cluster state failed.
     */
    public ClusterState state() throws GridClientException;

    /**
     * Changes cluster state to {@code newState}.
     *
     * @param newState New cluster state.
     * @param forceDeactivation If {@code true}, cluster deactivation will be forced.
     * @throws GridClientException If the request to change the cluster state failed.
     * @see ClusterState#INACTIVE
     */
    public void state(ClusterState newState, boolean forceDeactivation) throws GridClientException;

    /**
     * Get the cluster name.
     *
     * @return The name of the cluster.
     * @throws GridClientException If the request to get the cluster name failed.
     * */
    String clusterName() throws GridClientException;
}
