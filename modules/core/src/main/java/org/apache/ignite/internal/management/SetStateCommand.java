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
import lombok.Data;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.management.api.ConfirmableCommand;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.management.api.Parameter;
import org.apache.ignite.internal.management.api.PositionalParameter;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
@Data
public class SetStateCommand extends ConfirmableCommand {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @PositionalParameter()
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
    @Parameter(optional = true, excludeFromDescription = true)
    private boolean force;

    /** {@inheritDoc} */
    @Override public String description() {
        return "Change cluster state";
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        super.writeExternalData(out);

        U.writeEnum(out, state);
        out.writeBoolean(force);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalData(protoVer, in);

        state = U.readEnum(in, ClusterState.class);
        force = in.readBoolean();
    }
}
