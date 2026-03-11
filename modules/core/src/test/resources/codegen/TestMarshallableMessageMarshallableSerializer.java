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

package org.apache.ignite.internal;

import org.apache.ignite.internal.TestMarshallableMessage;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public class TestMarshallableMessageMarshallableSerializer implements MessageSerializer<TestMarshallableMessage> {
    /** */
    private final ClassLoader clsLdr;
    /** */
    private final Marshaller marshaller;

    /** */
    public TestMarshallableMessageMarshallableSerializer(Marshaller marshaller, ClassLoader clsLdr) {
        this.marshaller = marshaller;
        this.clsLdr = clsLdr;
    }
    /** */
    @Override public boolean writeTo(TestMarshallableMessage msg, MessageWriter writer) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            msg.prepareMarshal(marshaller);

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeInt(msg.iv))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeString(msg.sv))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeByteArray(msg.cstDataBytes))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public boolean readFrom(TestMarshallableMessage msg, MessageReader reader) {
        switch (reader.state()) {
            case 0:
                msg.iv = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.sv = reader.readString();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                msg.cstDataBytes = reader.readByteArray();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        msg.finishUnmarshal(marshaller, clsLdr);

        return true;
    }
}