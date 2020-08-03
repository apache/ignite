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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Base MVCC version implementation.
 */
public class MvccVersionImpl implements MvccVersion, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Coordinator version. */
    private long crdVer;

    /** Local counter. */
    private long cntr;

    /** Operation counter. */
    private int opCntr;

    /**
     * Constructor.
     */
    public MvccVersionImpl() {
        // No-op.
    }

    /**
     * @param crdVer Coordinator version.
     * @param cntr Counter.
     * @param opCntr Operation counter.
     */
    public MvccVersionImpl(long crdVer, long cntr, int opCntr) {
        this.crdVer = crdVer;
        this.cntr = cntr;
        this.opCntr = opCntr;
    }

    /**
     * @return Coordinator version.
     */
    @Override public long coordinatorVersion() {
        return crdVer;
    }

    /**
     * @return Local counter.
     */
    @Override public long counter() {
        return cntr;
    }

    /** {@inheritDoc} */
    @Override public int operationCounter() {
        return opCntr;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        MvccVersionImpl that = (MvccVersionImpl) o;

        return crdVer == that.crdVer && cntr == that.cntr;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = (int) (crdVer ^ (crdVer >>> 32));

        res = 31 * res + (int) (cntr ^ (cntr >>> 32));

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeLong("cntr", cntr))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("crdVer", crdVer))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeInt("opCntr", opCntr))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                cntr = reader.readLong("cntr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                crdVer = reader.readLong("crdVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                opCntr = reader.readInt("opCntr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(MvccVersionImpl.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 148;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccVersionImpl.class, this);
    }
}
