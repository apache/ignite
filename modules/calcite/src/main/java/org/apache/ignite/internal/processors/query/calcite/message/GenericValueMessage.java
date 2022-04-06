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

package org.apache.ignite.internal.processors.query.calcite.message;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public final class GenericValueMessage implements ValueMessage {
    /** */
    @GridDirectTransient
    private Object val;

    /** */
    private byte[] serialized;

    /** */
    public GenericValueMessage() {

    }

    /** */
    public GenericValueMessage(Object val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public Object value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(MarshallingContext ctx) throws IgniteCheckedException {
        if (val != null && serialized == null)
            serialized = ctx.marshal(val);
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(MarshallingContext ctx) throws IgniteCheckedException {
        if (serialized != null && val == null)
            val = ctx.unmarshal(serialized);
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
                if (!writer.writeByteArray("serialized", serialized))
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
                serialized = reader.readByteArray("serialized");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GenericValueMessage.class);
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.GENERIC_VALUE_MESSAGE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }
}
