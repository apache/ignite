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
     * Changes Ignite grid state to active or inactive.
     * Fails if the operation is not safe.
     * @see ClusterState#INACTIVE
     *
     * @param active {@code True} activate, {@code False} deactivate.
     * @deprecated Use {@link #state(ClusterState, boolean)} instead.
     */
    @Deprecated
    public void active(boolean active) throws GridClientException;

    /**
     * @return {@code Boolean} - Current cluster state. {@code True} active, {@code False} inactive.
     * @deprecated Use {@link #state(ClusterState)} instead.
     */
    @Deprecated
    public boolean active() throws GridClientException;

    /**
     * @return Current cluster state.
     * @throws GridClientException If the request to get the cluster state failed.
     */
    public ClusterState state() throws GridClientException;

    /**
     * Changes cluster state to {@code newState}.
     * Fails if the operation is not safe.
     * @see ClusterState#INACTIVE
     *
     * @param newState New cluster state.
     * @throws GridClientException If the request to change the cluster state failed.
     * @deprecated Use {@link #state(ClusterState, boolean)} instead.
     */
    @Deprecated
    public void state(ClusterState newState) throws GridClientException;

    /**
     * Changes cluster state to {@code newState}. Fails if the operation is not safe and {@code force}
     * is {@code False}.
     * <p>
     * <b>NOTE:</b>
     * After cluster deactivation all data from in-memory cache will be lost.
     * @see ClusterState#INACTIVE
     *
     * @param newState New cluster state.
     * @param force New cluster state.
     * @throws GridClientException If the request to change the cluster state failed.
     */
    public void state(ClusterState newState, boolean force) throws GridClientException;

    /**
     * Get the cluster name.
     *
     * @return The name of the cluster.
     * @throws GridClientException If the request to get the cluster name failed.
     * */
    String clusterName() throws GridClientException;
}
