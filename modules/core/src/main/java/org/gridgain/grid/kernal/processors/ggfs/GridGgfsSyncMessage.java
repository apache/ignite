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

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;

/**
 * Basic sync message.
 */
public class GridGgfsSyncMessage extends GridGgfsCommunicationMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Coordinator node order. */
    private long order;

    /** Response flag. */
    private boolean res;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridGgfsSyncMessage() {
        // No-op.
    }

    /**
     * @param order Node order.
     * @param res Response flag.
     */
    public GridGgfsSyncMessage(long order, boolean res) {
        this.order = order;
        this.res = res;
    }

    /**
     * @return Coordinator node order.
     */
    public long order() {
        return order;
    }

    /**
     * @return {@code True} if response message.
     */
    public boolean response() {
        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsSyncMessage.class, this);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridGgfsSyncMessage _clone = new GridGgfsSyncMessage();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridGgfsSyncMessage _clone = (GridGgfsSyncMessage)_msg;

        _clone.order = order;
        _clone.res = res;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putLong(order))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putBoolean(res))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 0:
                if (buf.remaining() < 8)
                    return false;

                order = commState.getLong();

                commState.idx++;

            case 1:
                if (buf.remaining() < 1)
                    return false;

                res = commState.getBoolean();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 72;
    }
}
