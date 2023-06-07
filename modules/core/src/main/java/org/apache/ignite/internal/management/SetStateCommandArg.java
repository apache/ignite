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

package org.apache.ignite.internal.management;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.CliConfirmArgument;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
@CliConfirmArgument
public class SetStateCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Positional
    @Argument
    @EnumDescription(
        names = {
            "ACTIVE",
            "INACTIVE",
            "ACTIVE_READ_ONLY"
        },
        descriptions = {
            "Activate cluster. Cache updates are allowed",
            "Deactivate cluster",
            "Activate cluster. Cache updates are denied"
        }
    )
    private ClusterState state;

    /** */
    @Argument(optional = true, description = "If true, cluster deactivation will be forced")
    private boolean force;

    /** */
    private String clusterName;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, state);
        out.writeBoolean(force);
        U.writeString(out, clusterName);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        state = U.readEnum(in, ClusterState.class);
        force = in.readBoolean();
        clusterName = U.readString(in);
    }

    /** */
    public ClusterState state() {
        return state;
    }

    /** */
    public void state(ClusterState state) {
        this.state = state;
    }

    /** */
    public boolean force() {
        return force;
    }

    /** */
    public void force(boolean force) {
        this.force = force;
    }

    /** */
    public String clusterName() {
        return clusterName;
    }

    /** */
    public void clusterName(String clusterName) {
        this.clusterName = clusterName;
    }
}
