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

import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.nio.*;

/**
 * Ping packet wrapper for direct marshalling.
 */
public class GridClientPingPacketWrapper extends MessageAdapter {
    /** */
    private static final long serialVersionUID = -3956036611004055629L;

    /** Ping message size (always zero). */
    private int size;

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf) {
        writer.setBuffer(buf);

        if (!typeWritten) {
            if (!writer.writeByte(null, directType()))
                return false;

            typeWritten = true;
        }

        switch (state) {
            case 0:
                if (!writer.writeInt("size", size))
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return GridClientMessageWrapper.REQ_HEADER;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        GridClientPingPacketWrapper _clone = new GridClientPingPacketWrapper();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        GridClientPingPacketWrapper _clone = (GridClientPingPacketWrapper)_msg;

        _clone.size = size;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientPingPacketWrapper.class, this);
    }
}
