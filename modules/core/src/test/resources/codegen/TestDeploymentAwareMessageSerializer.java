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

import org.apache.ignite.internal.TestDeploymentAwareMessage;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public final class TestDeploymentAwareMessageSerializer implements MessageSerializer<TestDeploymentAwareMessage> {
    /** */
    @Override public final boolean writeTo(TestDeploymentAwareMessage msg, MessageWriter writer) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeByteArray(msg.dataBytes))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage(msg.depInfo))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeString(msg.clsName))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public final boolean readFrom(TestDeploymentAwareMessage msg, MessageReader reader) {
        switch (reader.state()) {
            case 0:
                msg.dataBytes = reader.readByteArray();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.depInfo = reader.readMessage();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                msg.clsName = reader.readString();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public final TestDeploymentAwareMessage createMessage() {
        return new TestDeploymentAwareMessage();
    }
}
