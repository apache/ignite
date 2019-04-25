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

package org.apache.ignite.internal.processors.rest.client.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 */
public class GridClientStateRequest extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Active. */
    private boolean active;

    /** Request current state. */
    private boolean reqCurrentState;

    /**
     *
     */
    public boolean active() {
        return active;
    }

    /**
     *
     */
    public void active(boolean active){
        this.active = active;
    }

    /**
     *
     */
    public void requestCurrentState() {
        this.reqCurrentState = true;
    }

    /**
     *
     */
    public boolean isReqCurrentState() {
        return reqCurrentState;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeBoolean(active);
        out.writeBoolean(reqCurrentState);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        active = in.readBoolean();
        reqCurrentState = in.readBoolean();
    }
}
