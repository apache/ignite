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

package org.apache.ignite.internal.codegen;

import java.nio.ByteBuffer;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.internal.ChildMessage;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.codegen.MessageSerializerGenerator
 */
public class ChildMessageSerializer implements MessageSerializer {
    /** */
    @Override public boolean writeTo(Message m, ByteBuffer buf, MessageWriter writer) {
        ChildMessage msg = (ChildMessage)m;

        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeInt(msg.id()))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeString(msg.str()))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** */
    @Override public boolean readFrom(Message m, ByteBuffer buf, MessageReader reader) {
        ChildMessage msg = (ChildMessage)m;

        reader.setBuffer(buf);

        switch (reader.state()) {
            case 0:
                msg.id(reader.readInt());

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.str(reader.readString());

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }
}