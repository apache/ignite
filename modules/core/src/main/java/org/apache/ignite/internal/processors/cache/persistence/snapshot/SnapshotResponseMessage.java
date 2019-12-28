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
 *
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class SnapshotResponseMessage extends AbstractSnapshotMessage {
    /** Snapshot response message type (value is {@code 178}). */
    public static final short TYPE_CODE = 178;

    /** Serialization version. */
    private static final long serialVersionUID = 0L;

    /** Exception occurred during snapshot processing. */
    private String errMsg;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public SnapshotResponseMessage() {
        // No-op.
    }

    /**
     * @param snpName Snapshot name to which response related to.
     * @param errMsg Response error message.
     */
    public SnapshotResponseMessage(String snpName, String errMsg) {
        super(snpName);

        this.errMsg = errMsg;
    }

    /**
     * @return Response error message.
     */
    public String errorMessage() {
        return errMsg;
    }

    /**
     * @param errMsg Response error message.
     * @return {@code this} for chaining.
     */
    public SnapshotResponseMessage errorMessage(String errMsg) {
        this.errMsg = errMsg;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        if (writer.state() == 1) {
            if (!writer.writeString("errMsg", errMsg))
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

        if (!super.readFrom(buf, reader))
            return false;

        if (reader.state() == 1) {
            errMsg = reader.readString("errMsg");

            if (!reader.isLastRead())
                return false;

            reader.incrementState();
        }

        return reader.afterMessageRead(SnapshotResponseMessage.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotResponseMessage.class, this, super.toString());
    }
}
