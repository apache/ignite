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

package org.apache.ignite.plugin.extensions.communication;

import java.io.*;
import java.nio.*;

/**
 * Base class for all communication messages.
 */
public abstract class MessageAdapter implements Serializable {
    /** Message reader. */
    protected MessageReader reader;

    /** Current read state. */
    protected int readState;

    /**
     * @param reader Message reader.
     */
    public final void setReader(MessageReader reader) {
        assert this.reader == null;
        assert reader != null;

        this.reader = reader;
    }

    /**
     * Writes this message to provided byte buffer.
     *
     * @param buf Byte buffer.
     * @param writer Writer.
     * @return Whether message was fully written.
     */
    public abstract boolean writeTo(ByteBuffer buf, MessageWriter writer);

    /**
     * Reads this message from provided byte buffer.
     *
     * @param buf Byte buffer.
     * @return Whether message was fully read.
     */
    public abstract boolean readFrom(ByteBuffer buf);

    /**
     * Gets message type.
     *
     * @return Message type.
     */
    public abstract byte directType();

    /**
     * Defines whether recovery for this message should be skipped.
     *
     * @return Whether recovery for this message should be skipped.
     */
    public boolean skipRecovery() {
        return false;
    }

    /**
     * Enum representing possible types of collection items.
     */
    public enum Type {
        /** Byte. */
        BYTE,

        /** Short. */
        SHORT,

        /** Integer. */
        INT,

        /** Long. */
        LONG,

        /** Float. */
        FLOAT,

        /** Double. */
        DOUBLE,

        /** Character. */
        CHAR,

        /** Boolean. */
        BOOLEAN,

        /** Byte array. */
        BYTE_ARR,

        /** Short array. */
        SHORT_ARR,

        /** Integer array. */
        INT_ARR,

        /** Long array. */
        LONG_ARR,

        /** Float array. */
        FLOAT_ARR,

        /** Double array. */
        DOUBLE_ARR,

        /** Character array. */
        CHAR_ARR,

        /** Boolean array. */
        BOOLEAN_ARR,

        /** String. */
        STRING,

        /** Bit set. */
        BIT_SET,

        /** UUID. */
        UUID,

        /** Ignite UUID. */
        IGNITE_UUID,

        /** Message. */
        MSG
    }
}
