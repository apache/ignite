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

package org.apache.ignite.internal.processors.cache.consistentcut;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedBaseMessage;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/** Message that holds a transaction finish message and {@link ConsistentCutMarker}. */
public class ConsistentCutMarkerTxFinishMessage extends ConsistentCutMarkerMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 203;

    /** Marker of the latest Consistent Cut AFTER which this transaction committed. */
    private ConsistentCutMarker txMarker;

    /** Empty constructor required for {@link Externalizable}. */
    public ConsistentCutMarkerTxFinishMessage() {
    }

    /** */
    public ConsistentCutMarkerTxFinishMessage(
        GridDistributedBaseMessage payload,
        ConsistentCutMarker marker,
        ConsistentCutMarker txMarker
    ) {
        super(payload, marker);

        this.txMarker = txMarker;
    }

    /** */
    public ConsistentCutMarker txMarker() {
        return txMarker;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
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

        switch (writer.state()) {
            case 6:
                if (!writer.writeMessage("txMarker", txMarker))
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

        switch (reader.state()) {
            case 6:
                txMarker = reader.readMessage("txMarker");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(ConsistentCutMarkerTxFinishMessage.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 7;
    }
}
