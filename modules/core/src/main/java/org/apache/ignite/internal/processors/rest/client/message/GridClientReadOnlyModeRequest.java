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
public class  GridClientReadOnlyModeRequest extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Request current state. */
    private boolean reqCurrentState;

    /** Read only. */
    private boolean readOnly;

    /** */
    public boolean isReqCurrentState() {
        return reqCurrentState;
    }

    /** */
    public boolean readOnly() {
        return readOnly;
    }

    /**
     * @return Current read-only mode request.
     */
    public static GridClientReadOnlyModeRequest currentReadOnlyMode() {
        GridClientReadOnlyModeRequest msg = new GridClientReadOnlyModeRequest();

        msg.reqCurrentState = true;

        return msg;
    }

    /**
     * @return Enable read-only mode request.
     */
    public static GridClientReadOnlyModeRequest enableReadOnly() {
        GridClientReadOnlyModeRequest msg = new GridClientReadOnlyModeRequest();

        msg.readOnly = true;

        return msg;
    }

    /**
     * @return Disable read-only mode request.
     */
    public static GridClientReadOnlyModeRequest disableReadOnly() {
        GridClientReadOnlyModeRequest msg = new GridClientReadOnlyModeRequest();

        msg.readOnly = false;

        return msg;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeBoolean(reqCurrentState);
        out.writeBoolean(readOnly);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        reqCurrentState = in.readBoolean();
        readOnly = in.readBoolean();
    }
}
