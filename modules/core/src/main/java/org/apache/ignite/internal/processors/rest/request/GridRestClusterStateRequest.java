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

package org.apache.ignite.internal.processors.rest.request;

import org.apache.ignite.cluster.ClusterState;

/**
 *
 */
public class GridRestClusterStateRequest extends GridRestRequest {
    /** Flag of forced cluster deactivation. */
    public static final String ARG_FORCE = "force";

    /** Request current state. */
    private boolean reqCurrentMode;

    /** New state. */
    private ClusterState state;

    /** If {@code true}, cluster deactivation will be forced. */
    private boolean forceDeactivation;

    /**
     * @param forceDeactivation If {@code true}, cluster deactivation will be forced.
     */
    public void forceDeactivation(boolean forceDeactivation) {
        this.forceDeactivation = forceDeactivation;
    }

    /** */
    public void reqCurrentMode() {
        reqCurrentMode = true;
    }

    /** */
    public boolean isReqCurrentMode() {
        return reqCurrentMode;
    }

    /**
     * @return {@code True} if cluster deactivation will be forced. {@code False} otherwise.
     * @see ClusterState#INACTIVE
     */
    public boolean forceDeactivation() {
        return forceDeactivation;
    }

    /** */
    public ClusterState state() {
        return state;
    }

    /**
     * Sets new cluster state to request.
     *
     * @param state New cluster state.
     * @throws NullPointerException If {@code state} is null.
     */
    public void state(ClusterState state) {
        if (state == null)
            throw new NullPointerException("State can't be null.");

        this.state = state;
    }
}
