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

package org.apache.ignite.internal.direct.stream.v4;

import java.util.UUID;
import org.apache.ignite.internal.direct.stream.v3.DirectByteBufferStreamImplV3;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

import static org.apache.ignite.internal.util.GridUnsafe.BIG_ENDIAN;

/**
 * Direct marshalling I/O stream (version 4).
 */
public class DirectByteBufferStreamImplV4 extends DirectByteBufferStreamImplV3 {
    /**
     * @param msgFactory Message factory.
     */
    public DirectByteBufferStreamImplV4(MessageFactory msgFactory) {
        super(msgFactory);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(UUID val) {
        switch (uuidState) {
            case 0:
                writeBoolean(val == null);

                if (!lastFinished || val == null)
                    return;

                uuidState++;

            case 1:
                writeUuidRaw(val);

                if (!lastFinished)
                    return;

                uuidState = 0;
        }
    }

    /** {@inheritDoc} */
    @Override public UUID readUuid() {
        switch (uuidState) {
            case 0:
                boolean isNull = readBoolean();

                if (!lastFinished || isNull)
                    return null;

                uuidState++;

            case 1:
                readUuidRaw();

                if (!lastFinished)
                    return null;

                uuidState = 0;
        }

        UUID val = new UUID(uuidMost, uuidLeast);

        uuidMost = 0;
        uuidLeast = 0;

        return val;
    }


    /** {@inheritDoc} */
    @Override public void writeIgniteUuid(IgniteUuid val) {
        switch (uuidState) {
            case 0:
                writeBoolean(val == null);

                if (!lastFinished || val == null)
                    return;

                uuidState++;

            case 1:
                writeUuidRaw(val.globalId());

                if (!lastFinished)
                    return;

                uuidState++;

            case 2:
                writeLong(val.localId());

                if (!lastFinished)
                    return;

                uuidState = 0;
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid readIgniteUuid() {
        switch (uuidState) {
            case 0:
                boolean isNull = readBoolean();

                if (!lastFinished || isNull)
                    return null;

                uuidState++;

            case 1:
                readUuidRaw();

                if (!lastFinished)
                    return null;

                uuidState++;

            case 2:
                uuidLocId = readLong();

                if (!lastFinished)
                    return null;

                uuidState = 0;
        }

        IgniteUuid val = new IgniteUuid(new UUID(uuidMost, uuidLeast), uuidLocId);

        uuidMost = 0;
        uuidLeast = 0;
        uuidLocId = 0;

        return val;
    }

    /** */
    private void writeUuidRaw(UUID val) {
        lastFinished = buf.remaining() >= 16;

        if (lastFinished) {
            int pos = buf.position();

            long off = baseOff + pos;

            if (BIG_ENDIAN) {
                GridUnsafe.putLongLE(heapArr, off, val.getMostSignificantBits());
                GridUnsafe.putLongLE(heapArr, off + 8, val.getLeastSignificantBits());
            }
            else {
                GridUnsafe.putLong(heapArr, off, val.getMostSignificantBits());
                GridUnsafe.putLong(heapArr, off + 8, val.getLeastSignificantBits());
            }

            buf.position(pos + 16);
        }
    }

    /** */
    private void readUuidRaw() {
        lastFinished = buf.remaining() >= 16;

        if (lastFinished) {
            int pos = buf.position();

            buf.position(pos + 16);

            long off = baseOff + pos;

            if (BIG_ENDIAN) {
                uuidMost = GridUnsafe.getLongLE(heapArr, off);
                uuidLeast = GridUnsafe.getLongLE(heapArr, off + 8);
            }
            else {
                uuidMost = GridUnsafe.getLong(heapArr, off);
                uuidLeast = GridUnsafe.getLong(heapArr, off + 8);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DirectByteBufferStreamImplV4.class, this);
    }
}
