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

package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/** */
public class GridH2DmlResponse implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final byte STATUS_OK = 0;

    /** */
    public static final byte STATUS_ERROR = 1;

    /** */
    public static final byte STATUS_ERR_KEYS = 2;

    /** */
    private long reqId;

    /** */
    private byte status;

    /** */
    private long updCnt;

    /** */
    private String err;

    /** */
    private Object[] errKeys;

    /**
     * Default constructor.
     */
    public GridH2DmlResponse() {
        // No-op.
    }

    /** */
    public GridH2DmlResponse(long reqId, byte status, long updCnt, Object[] errKeys, String error) {
        this.reqId = reqId;
        this.status = status;
        this.updCnt = updCnt;
        this.errKeys = errKeys;
        this.err = error;
    }

    /**
     * @return Request id.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @return Status.
     */
    public byte status() {
        return status;
    }

    /**
     * @return Update counter.
     */
    public long updateCounter() {
        return updCnt;
    }

    /**
     * @return Error keys.
     */
    public Object[] errorKeys() {
        return errKeys;
    }

    /**
     * @return Error message.
     */
    public String error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridH2DmlResponse.class, this);
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
                if (!writer.writeString("err", err))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeObjectArray("errKeys", errKeys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeLong("reqId", reqId))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeByte("status", status))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeLong("updCnt", updCnt))
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
                err = reader.readString("err");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                errKeys = reader.readObjectArray("errKeys", MessageCollectionItemType.MSG, Object.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                reqId = reader.readLong("reqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                status = reader.readByte("status");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                updCnt = reader.readLong("updCnt");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridH2DmlResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -56;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 5;
    }

    @Override public void onAckReceived() {
        // No-op
    }
}

