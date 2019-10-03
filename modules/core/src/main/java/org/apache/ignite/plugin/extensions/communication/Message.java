/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.plugin.extensions.communication;

import java.io.Serializable;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;

/**
 * Base class for all communication messages.
 */
public interface Message extends Serializable {
    /** Direct type size in bytes. */
    public int DIRECT_TYPE_SIZE = 2;

    /**
     * Writes this message to provided byte buffer.
     *
     * @param buf Byte buffer.
     * @param writer Writer.
     * @return Whether message was fully written.
     */
    public boolean writeTo(ByteBuffer buf, MessageWriter writer);

    /**
     * Reads this message from provided byte buffer.
     *
     * @param buf Byte buffer.
     * @param reader Reader.
     * @return Whether message was fully read.
     */
    public boolean readFrom(ByteBuffer buf, MessageReader reader);

    /**
     * Gets message type.
     *
     * @return Message type.
     */
    public short directType();

    /**
     * Gets fields count.
     *
     * @return Fields count.
     */
    public byte fieldsCount();

    /**
     * Method called when ack message received.
     */
    public void onAckReceived();

    /**
     * Message processing policy.
     * Overrides processing policy defined by sender.
     *
     * @return Processing policy.
     */
    public default byte policy() {
        return GridIoPolicy.UNDEFINED;
    }

}