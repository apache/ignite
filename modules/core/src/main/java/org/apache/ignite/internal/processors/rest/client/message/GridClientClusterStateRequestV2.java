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

package org.apache.ignite.internal.processors.rest.client.message;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.cluster.ClusterState;

/**
 * Enchanced version of {@link GridClientClusterStateRequest}.
 * Introduced to support forced version of the change state command and keep backward compatibility
 * with nodes of old version that may occur in cluster at the rolling updates.
 */
public class GridClientClusterStateRequestV2 extends GridClientClusterStateRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** If {@code True}, cluster deactivation will be forced. */
    private boolean forceDeactivation;

    /**
     * @param state New cluster state.
     * @param forceDeactivation Forced cluster deactivation.
     * @return Cluster state change request.
     */
    public static GridClientClusterStateRequestV2 state(ClusterState state, boolean forceDeactivation) {
        return new GridClientClusterStateRequestV2(GridClientClusterStateRequest.state(state), forceDeactivation);
    }

    /** Empty constructor required by {@link Externalizable}. */
    public GridClientClusterStateRequestV2() {
        // No op.
    }

    /**
     * Copying constructor.
     *
     * @param clusterStateReq Original request, of the previous version.
     * @param forceDeactivation If {@code True}, indicates to skip checking of deactivation safety.
     */
    private GridClientClusterStateRequestV2(GridClientClusterStateRequest clusterStateReq, boolean forceDeactivation) {
        super(clusterStateReq);

        this.forceDeactivation = forceDeactivation;
    }

    /**
     * @return {@code True} if there is no need to ensure deactivation is safe.
     */
    public boolean forceDeactivation() {
        return forceDeactivation;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeBoolean(forceDeactivation);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        forceDeactivation = in.readBoolean();
    }
}
