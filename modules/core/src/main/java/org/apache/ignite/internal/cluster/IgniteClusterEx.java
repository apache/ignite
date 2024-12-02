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

package org.apache.ignite.internal.cluster;

import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;

/**
 *
 */
public interface IgniteClusterEx extends IgniteCluster, ClusterGroupEx {
    /**
     * Changes current cluster state to given {@code newState} cluster state.
     * <p>
     * <b>NOTE:</b>
     * Deactivation clears in-memory caches (without persistence) including the system caches.
     *
     * @param newState New cluster state.
     * @param force Force deactivation.
     * @throws IgniteException If there is an already started transaction or lock in the same thread.
     */
    public void state(ClusterState newState, boolean force) throws IgniteException;
}
