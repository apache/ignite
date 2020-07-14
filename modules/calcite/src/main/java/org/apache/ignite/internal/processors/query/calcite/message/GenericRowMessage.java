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
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class GenericRowMessage implements MarshalableMessage {
    /** */
    @GridDirectTransient
    private Object row;

    /** */
    private byte[] serRow;

    /** */
    public GenericRowMessage() {

    }

    /** */
    public GenericRowMessage(Object row) {
        this.row = row;
    }

    /** */
    public Object row() {
        return row;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marshaller) throws IgniteCheckedException {
        if (row != null && serRow == null)
            serRow = marshaller.marshal(row);
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(Marshaller marshaller, ClassLoader loader) throws IgniteCheckedException {
        if (serRow != null && row == null)
            row = marshaller.unmarshal(serRow, loader);
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
                if (!writer.writeByteArray("serRow", serRow))
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
                serRow = reader.readByteArray("serRow");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GenericRowMessage.class);
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.GENERIC_ROW_MESSAGE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }
}
